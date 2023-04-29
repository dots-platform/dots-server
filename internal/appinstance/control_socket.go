package appinstance

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/dtrust-project/dtrust-server/internal/util"
)

type controlMsgType uint16

const (
	// controlMsgTypeRequestSocket = 1 // Deprecated.
	controlMsgTypeMsgSend     controlMsgType = 2
	controlMsgTypeMsgRecv                    = 3
	controlMsgTypeMsgRecvResp                = 4
	controlMsgTypeOutput                     = 5
)

func (t controlMsgType) String() string {
	switch t {
	case controlMsgTypeMsgSend:
		return "MSG_SEND"
	case controlMsgTypeMsgRecv:
		return "MSG_RECV"
	case controlMsgTypeMsgRecvResp:
		return "MSG_RECV_RESP"
	case controlMsgTypeOutput:
		return "OUTPUT"
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

type controlMsgDataOutput struct{}

type appMsgTag struct {
	Tag int
}

func init() {
	gob.Register(appMsgTag{})
}

func (instance *AppInstance) sendFile(file *os.File) error {
	unixRights := syscall.UnixRights(int(file.Fd()))
	if err := syscall.Sendmsg(int(instance.controlSocket.Fd()), nil, unixRights, nil, 0); err != nil {
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

func (instance *AppInstance) handleOutputControlMsg(ctx context.Context, payload []byte) {
	instance.outputBuf.Write(payload)
}

func (instance *AppInstance) handleControlMsg(ctx context.Context, controlSocket *os.File, msg *controlMsg, payload []byte) {
	ctx = util.ContextWithLogger(ctx, util.LoggerFromContext(ctx).With(
		"cmd", msg.Type,
		"cmdPayloadLen", msg.PayloadLen,
	))
	util.LoggerFromContext(ctx).Debug("Received control command")

	// Dispatch command.
	switch msg.Type {
	case controlMsgTypeMsgSend:
		instance.handleMsgSendControlMsg(ctx, msg, payload)
	case controlMsgTypeMsgRecv:
		instance.handleMsgRecvControlMsg(ctx, msg)
	case controlMsgTypeOutput:
		instance.handleOutputControlMsg(ctx, payload)
	default:
		util.LoggerFromContext(ctx).Warn("Application issued invalid control message type")
	}
}

func (instance *AppInstance) manageControlSocket(ctx context.Context, controlSocket *os.File) {
	instance.controlSocket = controlSocket

	for {
		controlMsg, payload, err := instance.recvControlMsg(ctx)
		if err != nil {
			break
		}
		instance.handleControlMsg(ctx, controlSocket, controlMsg, payload)
	}
}
