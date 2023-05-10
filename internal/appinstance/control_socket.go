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

	"github.com/google/uuid"

	"github.com/dtrust-project/dots-server/internal/util"
)

type controlMsgType uint16

const (
	// controlMsgTypeRequestSocket = 1 // Deprecated.
	controlMsgTypeMsgSend       controlMsgType = 2
	controlMsgTypeMsgRecv                      = 3
	controlMsgTypeMsgRecvResp                  = 4
	controlMsgTypeOutput                       = 5
	controlMsgTypeReqAccept                    = 6
	controlMsgTypeReqAcceptResp                = 7
	controlMsgTypeReqFinish                    = 8
)

type controlResult struct {
	output []byte
}

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
	case controlMsgTypeReqAccept:
		return "REQ_ACCEPT"
	case controlMsgTypeReqAcceptResp:
		return "REQ_ACCEPT_RESP"
	case controlMsgTypeReqFinish:
		return "REQ_FINISH"
	default:
		return fmt.Sprintf("INVALID: 0x%04x", uint16(t))
	}
}

type controlMsg struct {
	MsgId      uint64
	RespMsgId  uint64
	Type       controlMsgType
	_          [2]byte
	PayloadLen uint32
	RequestId  uuid.UUID
	_          [24]byte
	Data       [64]byte
}

func init() {
	if binary.Size(&controlMsg{}) != 128 {
		panic("Control message length must be 128 bytes long")
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
	RequestId uuid.UUID
	Tag       int
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

	msg.MsgId = instance.controlSocketMsgCounter.Add(1)

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

func (instance *AppInstance) handleMsgSendControlMsg(ctx context.Context, req *AppRequest, msg *controlMsg, payload []byte) {
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
	instance.serverComm.Send(recipientId, appMsgTag{msg.RequestId, int(data.Tag)}, payload)
}

func (instance *AppInstance) handleMsgRecvControlMsg(ctx context.Context, req *AppRequest, msg *controlMsg, payload []byte) {
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
		recvPayload, err := instance.serverComm.Recv(senderId, appMsgTag{msg.RequestId, int(data.Tag)})
		if err != nil {
			util.LoggerFromContext(ctx).Error("Failed to receive", "err", err)
			return
		}
		payload, ok := recvPayload.([]byte)
		if !ok {
			util.LoggerFromContext(ctx).Error("Received message was not byte slice")
			return
		}

		respMsg := controlMsg{
			RespMsgId: msg.MsgId,
			Type:      controlMsgTypeMsgRecvResp,
		}
		if err := instance.sendControlMsg(ctx, &respMsg, payload); err != nil {
			util.LoggerFromContext(ctx).Error("Error sending MSG_RECV response data", "err", err)
			return
		}
	}()
}

func (instance *AppInstance) handleOutputControlMsg(ctx context.Context, req *AppRequest, msg *controlMsg, payload []byte) {
	if req == nil {
		util.LoggerFromContext(ctx).Warn("Application issued OUTPUT command with nil request")
		return
	}
	req.outputBuf.Write(payload)
}

func (instance *AppInstance) handleReqAcceptControlMsg(ctx context.Context, req *AppRequest, msg *controlMsg, payload []byte) {
	go func() {
		nextReq := <-instance.pendingRequests

		// Fixed header inputs.
		var reqInputHeader appReqInput
		reqInputOffset := uint32(0)
		reqInputHeader.Id = nextReq.Id
		reqInputOffset += uint32(binary.Size(&reqInputHeader))
		reqInputBytes := make([]byte, reqInputOffset)

		// Function name.
		reqInputHeader.FuncNameOffset = reqInputOffset
		reqInputBytes = append(reqInputBytes, []byte(nextReq.FuncName)...)
		reqInputBytes = append(reqInputBytes, 0)
		reqInputOffset += uint32(len(nextReq.FuncName) + 1)

		// Exec arguments.
		reqInputHeader.ArgsOffset = reqInputOffset
		reqInputHeader.ArgsCount = uint32(len(nextReq.Args))
		reqInputArgOffset := uint32(len(nextReq.Args) * binary.Size(&appEnvIovec{}))
		reqInputArgInputBuf := new(bytes.Buffer)
		for _, arg := range nextReq.Args {
			reqInputArgIovec := appEnvIovec{
				Offset: reqInputOffset + reqInputArgOffset,
				Length: uint32(len(arg)),
			}
			if err := binary.Write(reqInputArgInputBuf, binary.BigEndian, &reqInputArgIovec); err != nil {
				util.LoggerFromContext(ctx).Error("Failed to marshal app request input arg iovec", "err", err)
				nextReq.done <- appResult{err: err}
				return
			}
			reqInputArgOffset += uint32(len(arg) + 1)
		}
		reqInputBytes = append(reqInputBytes, reqInputArgInputBuf.Bytes()...)
		for _, arg := range nextReq.Args {
			reqInputBytes = append(reqInputBytes, arg...)
			reqInputBytes = append(reqInputBytes, 0)
		}
		reqInputOffset += reqInputArgOffset

		// Marshal the header and place it at the beginning of the input slice.
		reqInputBytesBuf := new(bytes.Buffer)
		if err := binary.Write(reqInputBytesBuf, binary.BigEndian, &reqInputHeader); err != nil {
			util.LoggerFromContext(ctx).Error("Failed to marshal app request input header", "err", err)
			nextReq.done <- appResult{err: err}
			return
		}
		copy(reqInputBytes, reqInputBytesBuf.Bytes())

		// Write response with request input as payload.
		respMsg := controlMsg{
			RespMsgId: msg.MsgId,
			Type: controlMsgTypeReqAcceptResp,
		}
		instance.sendControlMsg(ctx, &respMsg, reqInputBytes)
	}()
}

func (instance *AppInstance) handleReqFinishControlMsg(ctx context.Context, req *AppRequest, msg *controlMsg, payload []byte) {
	if req == nil {
		util.LoggerFromContext(ctx).Warn("Application issued REQ_FINISH command with nil request")
		return
	}

	req.done <- appResult{req.outputBuf.Bytes(), nil}

	func() {
		instance.requestsMutex.Lock()
		defer instance.requestsMutex.Unlock()
		delete(instance.requests, req.Id)
	}()
}

func (instance *AppInstance) handleControlMsg(ctx context.Context, req *AppRequest, msg *controlMsg, payload []byte) {
	util.LoggerFromContext(ctx).Debug("Received control command")

	// Dispatch command.
	switch msg.Type {
	case controlMsgTypeMsgSend:
		instance.handleMsgSendControlMsg(ctx, req, msg, payload)
	case controlMsgTypeMsgRecv:
		instance.handleMsgRecvControlMsg(ctx, req, msg, payload)
	case controlMsgTypeOutput:
		instance.handleOutputControlMsg(ctx, req, msg, payload)
	case controlMsgTypeReqAccept:
		instance.handleReqAcceptControlMsg(ctx, req, msg, payload)
	case controlMsgTypeReqFinish:
		instance.handleReqFinishControlMsg(ctx, req, msg, payload)
	default:
		util.LoggerFromContext(ctx).Warn("Application issued invalid control message type")
	}
}

func (instance *AppInstance) manageControlSocket(ctx context.Context) {
	for {
		controlMsg, payload, err := instance.recvControlMsg(context.Background())
		if err != nil {
			break
		}
		msgCtx := ctx

		msgCtx = util.ContextWithLogger(msgCtx, util.LoggerFromContext(ctx).With(
			"cmd", controlMsg.Type,
			"cmdPayloadLen", controlMsg.PayloadLen,
		))

		// Try to get the request for this message.
		var req *AppRequest
		if controlMsg.RequestId != uuid.Nil {
			msgCtx = util.ContextWithLogger(msgCtx, util.LoggerFromContext(msgCtx).With(
				"requestId", controlMsg.RequestId,
			))

			var ok bool
			func() {
				instance.requestsMutex.RLock()
				defer instance.requestsMutex.RUnlock()
				req, ok = instance.requests[controlMsg.RequestId]
			}()
			if !ok {
				util.LoggerFromContext(msgCtx).Warn("Application issued command for invalid request ID")
				continue
			}
		}
		if req != nil {
			msgCtx = req.ctx
			msgCtx = util.ContextWithLogger(msgCtx, util.LoggerFromContext(ctx).With(
				"cmd", controlMsg.Type,
				"cmdPayloadLen", controlMsg.PayloadLen,
				"requestId", controlMsg.RequestId,
			))
		}

		instance.handleControlMsg(msgCtx, req, controlMsg, payload)
	}
}
