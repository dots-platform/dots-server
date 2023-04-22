package appinstance

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/avast/retry-go"
	"github.com/dtrust-project/dtrust-server/internal/util"
)

type controlMsgType uint16

const (
	controlMsgTypeRequestSocket controlMsgType = 1
	controlMsgTypeMsgSend                      = 2
	controlMsgTypeMsgRecv                      = 3
	controlMsgTypeMsgRecvResp                  = 4
)

func (t controlMsgType) String() string {
	switch t {
	case controlMsgTypeRequestSocket:
		return "REQUEST_SOCKET"
	case controlMsgTypeMsgSend:
		return "MSG_SEND"
	case controlMsgTypeMsgRecv:
		return "MSG_RECV"
	case controlMsgTypeMsgRecvResp:
		return "MSG_RECV_RESP"
	default:
		return fmt.Sprintf("INVALID: 0x%04x", uint16(t))
	}
}

type controlMsg struct {
	Type       controlMsgType
	_          uint16
	PayloadLen uint32
	_          [24]byte
	Data       [32]byte
}

func init() {
	if binary.Size(&controlMsg{}) != 64 {
		panic("Control message length must be 64 bytes long")
	}
}

type controlMsgDataRequestSocket struct {
	OtherRank uint32
}

type controlMsgDataMsgSend struct {
	Recipient uint32
	Tag       uint32
}

type controlMsgDataMsgRecv struct {
	Sender uint32
	Tag    uint32
}

type appMsgTag struct {
	Tag int
}

func init() {
	gob.Register(appMsgTag{})
}

func (instance *AppInstance) sendFile(file *os.File) error {
	controlFile, err := instance.controlSocket.File()
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

func (instance *AppInstance) sendControlMsg(ctx context.Context, msg *controlMsg, payload []byte) error {
	instance.controlSocketSendMutex.Lock()
	defer instance.controlSocketSendMutex.Unlock()

	// Send header.
	msg.PayloadLen = uint32(len(payload))
	if err := binary.Write(instance.controlSocket, binary.BigEndian, msg); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to write control message", "err", err)
		return err
	}

	// Send payload.
	if _, err := instance.controlSocket.Write(payload); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to write control message payload", "err", err)
		return err
	}

	return nil
}

func (instance *AppInstance) recvControlMsg(ctx context.Context) (*controlMsg, []byte, error) {
	instance.controlSocketRecvMutex.Lock()
	defer instance.controlSocketRecvMutex.Unlock()

	// Read header.
	var msg controlMsg
	if err := binary.Read(instance.controlSocket, binary.BigEndian, &msg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		util.LoggerFromContext(ctx).Error("Failed to read control message", "err", err)
		return nil, nil, err
	}

	// Read payload.
	var payload []byte
	if msg.PayloadLen > 0 {
		payload = make([]byte, msg.PayloadLen)
		if _, err := io.ReadFull(instance.controlSocket, payload); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil, err
			}
			util.LoggerFromContext(ctx).Error("Failed to read control message payload", "err", err)
			return nil, nil, err
		}
	}

	return &msg, payload, nil
}

func (instance *AppInstance) handleRequestSocketControlMsg(ctx context.Context, msg *controlMsg) {
	var data controlMsgDataRequestSocket
	dataReader := bytes.NewReader(msg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to unmarshal binary data", "err", err)
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
		listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
		if err != nil {
			util.LoggerFromContext(ctx).Error("Failed to listen", "err", err)
			return
		}
		defer listener.Close()
		conn, err = listener.Accept()
		if err != nil {
			util.LoggerFromContext(ctx).Error("Failed to listen", "err", err)
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
			util.LoggerFromContext(ctx).Error("Failed to dial", "err", err)
		}
	}
	defer conn.Close()

	// Pass socket through control socket.
	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to get TCP file", "err", err)
		return
	}
	defer file.Close()
	if err := instance.sendFile(file); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to get send TCP through control socket", "err", err)
		return
	}
}

func (instance *AppInstance) handleMsgSendControlMsg(ctx context.Context, msg *controlMsg, payload []byte) {
	var data controlMsgDataMsgSend
	dataReader := bytes.NewReader(msg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to unmarshal binary data", "err", err)
		return
	}

	if int(data.Recipient) >= len(instance.config.NodeIds) {
		util.LoggerFromContext(ctx).Warn("Application issued invalid command arguments")
		return
	}

	recipientId := instance.config.NodeIds[data.Recipient]
	// TODO Probably want to wrap this interface or something.
	instance.serverComm.Send(recipientId, appMsgTag{int(data.Tag)}, payload)
}

func (instance *AppInstance) handleMsgRecvControlMsg(ctx context.Context, msg *controlMsg) {
	var data controlMsgDataMsgRecv
	dataReader := bytes.NewReader(msg.Data[:])
	if err := binary.Read(dataReader, binary.BigEndian, &data); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to unmarshal binary data", "err", err)
		return
	}

	if int(data.Sender) >= len(instance.config.NodeIds) {
		util.LoggerFromContext(ctx).Warn("Application issued invalid command arguments")
		return
	}

	go func() {
		senderId := instance.config.NodeIds[data.Sender]
		recvPayload, err := instance.serverComm.Recv(senderId, appMsgTag{int(data.Tag)})
		if err != nil {
			util.LoggerFromContext(ctx).Error("Failed to receive", "err", err)
			return
		}
		payload := recvPayload.([]byte)

		respMsg := controlMsg{
			Type: controlMsgTypeMsgRecvResp,
		}
		if err := instance.sendControlMsg(ctx, &respMsg, payload); err != nil {
			util.LoggerFromContext(ctx).Error("Error sending MSG_RECV response data", "err", err)
			return
		}
	}()
}

func (instance *AppInstance) handleControlMsg(ctx context.Context, controlSocket *net.UnixConn, msg *controlMsg, payload []byte) {
	cmdCtx := util.ContextWithLogger(ctx, util.LoggerFromContext(ctx).With(
		"cmd", msg.Type,
		"cmdPayloadLen", msg.PayloadLen,
	))
	util.LoggerFromContext(cmdCtx).Debug("Received control command")

	// Dispatch command.
	switch msg.Type {
	case controlMsgTypeRequestSocket:
		instance.handleRequestSocketControlMsg(cmdCtx, msg)
	case controlMsgTypeMsgSend:
		instance.handleMsgSendControlMsg(cmdCtx, msg, payload)
	case controlMsgTypeMsgRecv:
		instance.handleMsgRecvControlMsg(cmdCtx, msg)
	default:
		util.LoggerFromContext(cmdCtx).Warn("Application issued invalid control message type")
	}
}

func (instance *AppInstance) manageControlSocket(ctx context.Context, controlSocket *net.UnixConn) {
	instance.controlSocket = controlSocket

	for {
		controlMsg, payload, err := instance.recvControlMsg(ctx)
		if err != nil {
			break
		}
		instance.handleControlMsg(ctx, controlSocket, controlMsg, payload)
	}
}
