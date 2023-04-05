package serverconn

import (
	"encoding/gob"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type serverMsg struct {
	Type      MsgType
	SubtypeId uuid.UUID
	Data      any
}

type ServerComm struct {
	msgType   MsgType
	subtypeId uuid.UUID
	buf       map[string]chan any
	conn      *ServerConn
	logger    log.FieldLogger
}

func init() {
	gob.Register(serverMsg{})
}

func (c *ServerComm) Send(nodeId string, tag any, data any) error {
	msgLog := c.logger.WithFields(log.Fields{
		"otherNodeId": nodeId,
	})
	msgLog.Debug("Sending server message")

	// TODO Handle tag.

	if err := c.conn.encoders[nodeId].Encode(&serverMsg{
		Type:      c.msgType,
		SubtypeId: c.subtypeId,
		Data:      data,
	}); err != nil {
		msgLog.WithError(err).Error("Failed to send server message")
		return err
	}

	return nil
}

func (c *ServerComm) Recv(nodeId string, tag any) (any, error) {
	msgLog := c.logger.WithFields(log.Fields{
		"otherNodeId": nodeId,
	})
	msgLog.Debug("Receiving server message")

	// TODO Handle tag.

	data := <-c.buf[nodeId]

	return data, nil
}
