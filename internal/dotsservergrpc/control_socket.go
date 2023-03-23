package dotsservergrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"
)

const ControlMsgSize = 64

type ControlMsgType uint16

const (
	ControlMsgTypeRequestSocket ControlMsgType = 1
)

func (t ControlMsgType) validate() error {
	if t < 1 || t > 1 {
		return errors.New("Invalid value for ControlMsgType")
	}
	return nil
}

func (t ControlMsgType) String() string {
	switch t {
	case ControlMsgTypeRequestSocket:
		return "REQUEST_SOCKET"
	default:
		panic("Unknown control message type")
	}
}

type ControlMsg struct {
	Type       ControlMsgType
	_          uint16
	PayloadLen uint32
	_          uint64
	Data       [48]byte
}

type ControlMsgDataRequestSocket struct {
	OtherRank uint32
}

func sendFile(ctx context.Context, controlSocket *net.UnixConn, file *os.File) error {
	controlFile, err := controlSocket.File()
	if err != nil {
		return err
	}
	defer controlFile.Close()

	unixRights := syscall.UnixRights(int(file.Fd()))
	if err := syscall.Sendmsg(int(controlFile.Fd()), nil, unixRights, nil, 0); err != nil {
		return err
	}

	return nil
}

func (s *DotsServerGrpc) handleRequestSocketControlMsg(ctx context.Context, controlSocket *net.UnixConn, controlMsg *ControlMsg, cmdLog log.FieldLogger) {
	var data ControlMsgDataRequestSocket
	dataReader := bytes.NewReader(controlMsg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		cmdLog.WithError(err).Error("Failed to unmarshal binary data")
	}

	ourRank := s.config.NodeRanks[s.nodeId]
	otherRank := int(data.OtherRank)

	// Construct TCP socket.
	var conn net.Conn
	ourConfig := s.config.Nodes[s.nodeId]
	otherConfig := s.config.Nodes[s.config.NodeIds[otherRank]]
	if ourRank < otherRank {
		// Act as the listener for higher ranks.
		var listenConfig net.ListenConfig
		listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
		if err != nil {
			cmdLog.WithError(err).Error("Failed to listen")
			return
		}
		defer listener.Close()
		conn, err = listener.Accept()
		if err != nil {
			cmdLog.WithError(err).Error("Failed to listen")
			return
		}
	} else {
		// Act as dialer for lower ranks.
		var dialer net.Dialer
		if err := retry.Do(
			func() error {
				c, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf(":%d", otherConfig.Ports[ourRank]))
				if err != nil {
					return err
				}
				conn = c
				return nil
			},
			retry.Context(ctx),
		); err != nil {
			cmdLog.WithError(err).Error("Failed to dial")
		}
	}
	defer conn.Close()

	// Pass socket through control socket.
	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		cmdLog.WithError(err).Error("Failed to get TCP file")
		return
	}
	defer file.Close()
	if err := sendFile(ctx, controlSocket, file); err != nil {
		cmdLog.WithError(err).Error("Failed to get send TCP through control socket")
		return
	}
}

func (s *DotsServerGrpc) manageControlSocket(ctx context.Context, appName string, funcName string, controlSocket *net.UnixConn) {
	execLog := log.WithFields(log.Fields{
		"appName":     appName,
		"appFuncName": funcName,
	})

	for {
		// Read header.
		var controlMsg ControlMsg
		if err := binary.Read(controlSocket, binary.BigEndian, &controlMsg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			execLog.WithError(err).Error("Failed to read control message")
			break
		}
		if err := controlMsg.Type.validate(); err != nil {
			execLog.WithError(err).Warn("Application issued invalid command type")
			break
		}

		cmdLog := execLog.WithFields(log.Fields{
			"command": controlMsg.Type.String(),
		})
		cmdLog.Debug("Received control command")

		// Dispatch command.
		switch controlMsg.Type {
		case ControlMsgTypeRequestSocket:
			s.handleRequestSocketControlMsg(ctx, controlSocket, &controlMsg, cmdLog)
		}
	}
}
