package dotsservergrpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"
)

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

func (s *DotsServerGrpc) handleRequestSocketControlMsg(ctx context.Context, controlSocket *net.UnixConn, cmd []string, cmdLog log.FieldLogger) {
	if len(cmd) != 3 {
		cmdLog.Warn("Application issued invalid command")
		return
	}
	firstRank, err := strconv.Atoi(cmd[1])
	if err != nil {
		cmdLog.Warn("Application issued invalid command")
		return
	}
	secondRank, err := strconv.Atoi(cmd[2])
	if err != nil {
		cmdLog.Warn("Application issued invalid command")
		return
	}

	ourRank := s.config.NodeRanks[s.nodeId]
	var otherRank int
	if firstRank != ourRank && secondRank != ourRank || firstRank == ourRank && secondRank == ourRank {
		cmdLog.Warn("Application issued invalid command")
		return
	} else if firstRank == ourRank {
		otherRank = secondRank
	} else {
		otherRank = firstRank
	}

	// Construct TCP socket.
	var conn net.Conn
	ourConfig := s.config.Nodes[s.nodeId]
	otherConfig := s.config.Nodes[s.config.NodeIds[otherRank]]
	if ourRank < otherRank {
		// Act as the listener for higher ranks.
		var listenConfig net.ListenConfig
		listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
		if err != nil {
			cmdLog.WithFields(log.Fields{
				"err": err,
			}).Error("Failed to listen")
			return
		}
		defer listener.Close()
		conn, err = listener.Accept()
		if err != nil {
			cmdLog.WithFields(log.Fields{
				"err": err,
			}).Error("Failed to listen")
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
			cmdLog.WithFields(log.Fields{
				"err": err,
			}).Error("Failed to dial")
		}
	}
	defer conn.Close()

	// Pass socket through control socket.
	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		cmdLog.WithFields(log.Fields{
			"err": err,
		}).Error("Failed to get TCP file")
		return
	}
	defer file.Close()
	if err := sendFile(ctx, controlSocket, file); err != nil {
		cmdLog.WithFields(log.Fields{
			"err": err,
		}).Error("Failed to get send TCP through control socket")
		return
	}
}

func (s *DotsServerGrpc) manageControlSocket(ctx context.Context, appName string, funcName string, controlSocket *net.UnixConn) {
	execLog := log.WithFields(log.Fields{
		"appName":     appName,
		"appFuncName": funcName,
	})

	lenBuf := make([]byte, 4)
	for {
		// Read length.
		bytesRead, err := controlSocket.Read(lenBuf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			execLog.WithFields(log.Fields{
				"err": err,
			}).Error("Failed to read control message length")
			break
		}
		if bytesRead < 4 {
			execLog.WithFields(log.Fields{
				"err": err,
			}).Warn("Application control message length corrupted")
			break
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// Read message.
		msgBuf := make([]byte, msgLen)
		controlSocket.Read(msgBuf)

		// Split message into arguments.
		cmd := strings.Split(string(msgBuf), " ")
		if len(cmd) < 1 {
			execLog.Warn("Application issued empty command")
		}

		cmdLog := execLog.WithFields(log.Fields{
			"command": cmd,
		})

		cmdLog.Debug("Received control command")

		// Dispatch command.
		switch cmd[0] {
		case "REQUEST_SOCKET":
			s.handleRequestSocketControlMsg(ctx, controlSocket, cmd, cmdLog)
		default:
			cmdLog.Warn("Application issued invalid command")
		}
	}
}
