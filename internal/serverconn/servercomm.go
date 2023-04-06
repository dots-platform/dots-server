package serverconn

import (
	"encoding/gob"
	"sync"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type serverMsg struct {
	Type      MsgType
	SubtypeId uuid.UUID
	Tag       any
	Data      any
}

type ServerComm struct {
	msgType   MsgType
	subtypeId uuid.UUID
	recvBuf   map[string]map[any]chan any
	recvMutex sync.Mutex
	conn      *ServerConn
	logger    log.FieldLogger
}

const recvBufSize = 256

func init() {
	gob.Register(serverMsg{})
}

func (c *ServerComm) Send(nodeId string, tag any, data any) error {
	msgLog := c.logger.WithFields(log.Fields{
		"otherNodeId": nodeId,
	})
	msgLog.Debug("Sending server message")

	if err := c.conn.encoders[nodeId].Encode(&serverMsg{
		Type:      c.msgType,
		SubtypeId: c.subtypeId,
		Tag:       tag,
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

	// TODO Figure out some accounting scheme to mitigate DoS attacks where the
	// attacker allocates a bunch of different tag buffered channels to cuase
	// OOM.

	var recvChan chan any
	func() {
		c.recvMutex.Lock()
		defer c.recvMutex.Unlock()

		r, ok := c.recvBuf[nodeId][tag]
		if !ok {
			r = make(chan any, recvBufSize)
			c.recvBuf[nodeId][tag] = r
		}
		recvChan = r
	}()
	data := <-recvChan

	return data, nil
}
