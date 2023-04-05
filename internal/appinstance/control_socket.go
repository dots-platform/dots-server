package appinstance

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
	ControlMsgTypeMsgSend                      = 2
	ControlMsgTypeMsgRecv                      = 3
	ControlMsgTypeMsgRecvResp                  = 4
)

func (t ControlMsgType) String() string {
	switch t {
	case ControlMsgTypeRequestSocket:
		return "REQUEST_SOCKET"
	case ControlMsgTypeMsgSend:
		return "MSG_SEND"
	case ControlMsgTypeMsgRecv:
		return "MSG_RECV"
	case ControlMsgTypeMsgRecvResp:
		return "MSG_RECV_RESP"
	default:
		return "INVALID"
	}
}

type ControlMsg struct {
	Type       ControlMsgType
	_          uint16
	PayloadLen uint32
	_          [24]byte
	Data       [32]byte
}

type ControlMsgDataRequestSocket struct {
	OtherRank uint32
}

type ControlMsgDataMsgSend struct {
	Recipient uint32
	Tag       uint32
}

type ControlMsgDataMsgRecv struct {
	Sender uint32
	Tag    uint32
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

func sendControlMsg(controlMsg *ControlMsg, payload []byte, controlSocket *net.UnixConn) error {
	// TODO Locking.

	// Send header.
	controlMsg.PayloadLen = uint32(len(payload))
	if err := binary.Write(controlSocket, binary.BigEndian, controlMsg); err != nil {
		return err
	}

	// Send payload.
	if _, err := controlSocket.Write(payload); err != nil {
		return err
	}

	return nil
}

func recvControlMsg(controlSocket *net.UnixConn, logger log.FieldLogger) (*ControlMsg, []byte, error) {
	// TODO Locking.

	// Read header.
	var controlMsg ControlMsg
	if err := binary.Read(controlSocket, binary.BigEndian, &controlMsg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		logger.WithError(err).Error("Failed to read control message")
		return nil, nil, err
	}

	// Read payload.
	var payload []byte
	if controlMsg.PayloadLen > 0 {
		payload = make([]byte, controlMsg.PayloadLen)
		if _, err := controlSocket.Read(payload); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil, err
			}
			logger.WithError(err).Error("Failed to read control message payload")
			return nil, nil, err
		}
	}

	return &controlMsg, payload, nil
}

func (instance *AppInstance) handleRequestSocketControlMsg(controlSocket *net.UnixConn, controlMsg *ControlMsg, cmdLog log.FieldLogger) {
	var data ControlMsgDataRequestSocket
	dataReader := bytes.NewReader(controlMsg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		cmdLog.WithError(err).Error("Failed to unmarshal binary data")
	}

	ourRank := instance.config.NodeRanks[instance.config.OurNodeId]
	otherRank := int(data.OtherRank)

	// Construct TCP socket.
	var conn net.Conn
	ourConfig := instance.config.Nodes[instance.config.OurNodeId]
	otherConfig := instance.config.Nodes[instance.config.NodeIds[otherRank]]
	if ourRank < otherRank {
		// Act as the listener for higher ranks.
		var listenConfig net.ListenConfig
		listener, err := listenConfig.Listen(instance.ctx, "tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
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
				c, err := dialer.DialContext(instance.ctx, "tcp", fmt.Sprintf(":%d", otherConfig.Ports[ourRank]))
				if err != nil {
					return err
				}
				conn = c
				return nil
			},
			retry.Context(instance.ctx),
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
	if err := sendFile(instance.ctx, controlSocket, file); err != nil {
		cmdLog.WithError(err).Error("Failed to get send TCP through control socket")
		return
	}
}

func (instance *AppInstance) handleMsgSendControlMsg(controlSocket *net.UnixConn, controlMsg *ControlMsg, payload []byte, cmdLog log.FieldLogger) {
	var data ControlMsgDataMsgSend
	dataReader := bytes.NewReader(controlMsg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		cmdLog.WithError(err).Error("Failed to unmarshal binary data")
		return
	}

	if int(data.Recipient) >= len(instance.config.NodeIds) {
		cmdLog.Warn("Application issued invalid command arguments")
		return
	}

	recipientId := instance.config.NodeIds[data.Recipient]
	select {
	// TODO Probably want to wrap this interface or something.
	case instance.serverConns[recipientId].Send <- payload:
	case <-instance.ctx.Done():
		return
	}
}

func (instance *AppInstance) handleMsgRecvControlMsg(controlSocket *net.UnixConn, controlMsg *ControlMsg, cmdLog log.FieldLogger) {
	var data ControlMsgDataMsgRecv
	dataReader := bytes.NewReader(controlMsg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		cmdLog.WithError(err).Error("Failed to unmarshal binary data")
		return
	}

	if int(data.Sender) >= len(instance.config.NodeIds) {
		cmdLog.Warn("Application issued invalid command arguments")
		return
	}

	senderId := instance.config.NodeIds[data.Sender]
	var payload []byte
	select {
	case recvPayload, ok := <-instance.serverConns[senderId].Recv:
		if !ok {
			cmdLog.Warn("Receive channel closed")
			return
		}
		payload = recvPayload.([]byte)
	case <-instance.ctx.Done():
		return
	}

	// Handle tags.
	respMsg := ControlMsg{
		Type: ControlMsgTypeMsgRecvResp,
	}
	if err := sendControlMsg(&respMsg, payload, controlSocket); err != nil {
		cmdLog.WithError(err).Error("Error sending MSG_RECV response data")
		return
	}
}

func (instance *AppInstance) manageControlSocket(appName string, funcName string, controlSocket *net.UnixConn) {
	execLog := log.WithFields(log.Fields{
		"appName":     appName,
		"appFuncName": funcName,
	})

	for {
		controlMsg, payload, err := recvControlMsg(controlSocket, execLog)
		if err != nil {
			break
		}

		cmdLog := execLog.WithFields(log.Fields{
			"command": controlMsg.Type,
		})
		cmdLog.Debug("Received control command")

		// Dispatch command.
		switch controlMsg.Type {
		case ControlMsgTypeRequestSocket:
			instance.handleRequestSocketControlMsg(controlSocket, controlMsg, cmdLog)
		case ControlMsgTypeMsgSend:
			instance.handleMsgSendControlMsg(controlSocket, controlMsg, payload, cmdLog)
		case ControlMsgTypeMsgRecv:
			instance.handleMsgRecvControlMsg(controlSocket, controlMsg, cmdLog)
		default:
			execLog.Warn("Application issued invalid control message type")
		}
	}
}
