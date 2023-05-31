// Copyright 2023 The Dots Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverconn

import (
	"encoding/gob"
	"errors"
	"sync"

	"golang.org/x/exp/slog"
)

type serverMsg struct {
	CommId any
	Tag    any
	Data   any
}

type ServerComm struct {
	commId    any
	recvBuf   map[string]map[any]chan any
	recvMutex sync.Mutex
	conn      *ServerConn
	logger    *slog.Logger
}

const recvBufSize = 256

func init() {
	gob.Register(serverMsg{})
}

func (c *ServerComm) Send(nodeId string, tag any, data any) error {
	msgLog := c.logger.With(
		"otherNodeId", nodeId,
		"serverTag", tag,
	)
	msgLog.Debug("Sending server message")

	if err := c.conn.encoders[nodeId].Encode(&serverMsg{
		CommId: c.commId,
		Tag:    tag,
		Data:   data,
	}); err != nil {
		msgLog.Error("Failed to send server message", "err", err)
		return err
	}

	return nil
}

func (c *ServerComm) Recv(nodeId string, tag any) (any, error) {
	msgLog := c.logger.With(
		"otherNodeId", nodeId,
		"serverTag", tag,
	)
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
	data, ok := <-recvChan
	if !ok {
		return nil, errors.New("Server communicator is closed")
	}

	return data, nil
}

type barrierTag struct {
	Tag any
}

func init() {
	gob.Register(barrierTag{})
}

func (c *ServerComm) Barrier(tag any) error {
	barrierLog := c.logger.With(
		"serverTag", tag,
	)
	barrierLog.Debug("Synchronizing barrier")

	for nodeId := range c.conn.config.Nodes {
		if nodeId == c.conn.config.OurNodeId {
			continue
		}
		if err := c.Send(nodeId, barrierTag{tag}, nil); err != nil {
			barrierLog.Error("Failed to send barrier message", "err", err)
			return err
		}
	}
	for nodeId := range c.conn.config.Nodes {
		if nodeId == c.conn.config.OurNodeId {
			continue
		}
		if _, err := c.Recv(nodeId, barrierTag{tag}); err != nil {
			barrierLog.Error("Failed to receive barrier message", "err", err)
			return err
		}
	}

	return nil
}
