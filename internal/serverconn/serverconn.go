package serverconn

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/avast/retry-go"
	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type MsgType uint16

const (
	MsgTypePlatform    MsgType = 1
	MsgTypeAppInstance         = 2
)

func (t MsgType) String() string {
	switch t {
	case MsgTypePlatform:
		return "PLATFORM"
	case MsgTypeAppInstance:
		return "APP_INSTANCE"
	default:
		return "INVALID"
	}
}

type ServerMsg struct {
	Type      MsgType
	SubtypeId uuid.UUID
	Data      any
}

type Channels struct {
	Send chan any
	Recv chan any
}

type ServerConn struct {
	connections map[string]net.Conn
	encoders    map[string]*gob.Encoder
	decoders    map[string]*gob.Decoder
	channels    map[MsgType]map[uuid.UUID]map[string]Channels
	mutex       sync.RWMutex
	config      *config.Config
	ctx         context.Context
}

const msgBufferSize = 256

func init() {
	gob.Register(ServerMsg{})
}

func (c *ServerConn) handleIncomingMessage(nodeId string, msg *ServerMsg) {
	msgLog := log.WithFields(log.Fields{
		"otherNodeId":  nodeId,
		"msgType":      msg.Type,
		"msgSubtypeId": msg.SubtypeId,
	})
	msgLog.Debug("Received server message")

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	typeChannels, ok := c.channels[msg.Type]
	if !ok {
		msgLog.Warn("Received message with no listener")
		return
	}

	channels, ok := typeChannels[msg.SubtypeId]
	if !ok {
		msgLog.Warn("Received message with no listener")
		return
	}

	select {
	case channels[nodeId].Recv <- msg.Data:
	default:
		msgLog.Warn("Listener receive buffer full; tearing down the connection")
		func() {
			c.mutex.RUnlock()
			defer c.mutex.RLock()
			c.Unregister(msg.Type, msg.SubtypeId)
		}()
	}
}

func (c *ServerConn) handleOutgoingMessage(nodeId string, msgType MsgType, id uuid.UUID, data any) {
	msgLog := log.WithFields(log.Fields{
		"otherNodeId":  nodeId,
		"msgType":      msgType,
		"msgSubtypeId": id,
	})
	msgLog.Debug("Sending server message")

	if err := c.encoders[nodeId].Encode(&ServerMsg{
		Type:      msgType,
		SubtypeId: id,
		Data:      data,
	}); err != nil {
		msgLog.WithError(err).Error("Failed to send server message")
		return
	}
}

func (c *ServerConn) receiveMessages(nodeId string, decoder *gob.Decoder) {
	connLog := log.WithFields(log.Fields{
		"otherNodeId": nodeId,
	})

	// Create goroutine to read from connection.
	msgChan := make(chan *ServerMsg)
	go func() {
		loop := true
		for loop {
			var serverMsg *ServerMsg
			if err := decoder.Decode(&serverMsg); err != nil {
				select {
				case <-c.ctx.Done():
				default:
					connLog.WithError(err).Error("Error reading from server connection")
				}
				loop = false
			}
			msgChan <- serverMsg
		}
	}()

	// Repeatedly receive messages and forward them to the approprate registered
	// channel.
	loop := true
	for loop {
		select {
		case msg := <-msgChan:
			c.handleIncomingMessage(nodeId, msg)
		case <-c.ctx.Done():
			loop = false
		}
	}
}

func (c *ServerConn) Establish(ctx context.Context, conf *config.Config) error {
	c.config = conf
	c.ctx = ctx

	// Set up pairwise TCP connections with all nodes.
	var dialer net.Dialer
	var listenConfig net.ListenConfig
	connChan := make(chan *struct {
		nodeId string
		conn   *net.TCPConn
	})
	errChan := make(chan error)
	ourRank := conf.NodeRanks[conf.OurNodeId]
	ourConfig := conf.Nodes[conf.OurNodeId]
	for nodeId, nodeConfig := range conf.Nodes {
		go func(nodeId string, nodeConfig *config.NodeConfig) {
			otherRank := conf.NodeRanks[nodeId]
			if ourRank == otherRank {
				return
			}

			var conn net.Conn
			if ourRank < otherRank {
				// Act as the listener for higher ranks.
				listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
				if err != nil {
					errChan <- err
					return
				}
				defer listener.Close()
				conn, err = listener.Accept()
				if err != nil {
					errChan <- err
					return
				}
			} else {
				// Act as dialer for lower ranks.
				if err := retry.Do(
					func() error {
						c, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf(":%d", nodeConfig.Ports[ourRank]))
						if err != nil {
							return err
						}
						conn = c
						return nil
					},
					retry.Context(ctx),
				); err != nil {
					errChan <- err
				}
			}

			// Extract socket and output.
			connChan <- &struct {
				nodeId string
				conn   *net.TCPConn
			}{
				nodeId: nodeId,
				conn:   conn.(*net.TCPConn),
			}
		}(nodeId, nodeConfig)
	}
	conns := make(map[string]net.Conn)
	var loopErr error
	for i := 0; i < len(conf.Nodes)-1; i++ {
		select {
		case s := <-connChan:
			conns[s.nodeId] = s.conn
		case err := <-errChan:
			loopErr = err
			log.WithError(err).Error("Error opening server-to-server socket")
		}
	}
	if loopErr != nil {
		return loopErr
	}

	// Build coders.
	encoders := make(map[string]*gob.Encoder)
	decoders := make(map[string]*gob.Decoder)
	for nodeId, conn := range conns {
		encoders[nodeId] = gob.NewEncoder(conn)
		decoders[nodeId] = gob.NewDecoder(conn)

		// Spawn receiver.
		go c.receiveMessages(nodeId, decoders[nodeId])
	}

	c.connections = conns
	c.encoders = encoders
	c.decoders = decoders
	c.channels = make(map[MsgType]map[uuid.UUID]map[string]Channels)

	return nil
}

func (c *ServerConn) Register(ctx context.Context, msgType MsgType, id uuid.UUID) (map[string]Channels, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Ensure channel doesn't already exist.
	typeChannels, ok := c.channels[msgType]
	if !ok {
		typeChannels = make(map[uuid.UUID]map[string]Channels)
		c.channels[msgType] = typeChannels
	}
	if _, ok := typeChannels[id]; ok {
		return nil, errors.New("Channel already exists for message type and ID")
	}

	// Make channel.
	channels := make(map[string]Channels)
	for nodeId := range c.config.Nodes {
		send := make(chan any)
		recv := make(chan any, msgBufferSize)

		// Spawn sender.
		go func(nodeId string) {
			for data := range send {
				c.handleOutgoingMessage(nodeId, msgType, id, data)
			}
		}(nodeId)

		channels[nodeId] = Channels{
			Send: send,
			Recv: recv,
		}
	}
	typeChannels[id] = channels

	return channels, nil
}

func (c *ServerConn) Unregister(msgType MsgType, id uuid.UUID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	typeChannels, ok := c.channels[msgType]
	if !ok {
		return
	}
	idChannels, ok := typeChannels[id]
	if !ok {
		return
	}
	for _, channels := range idChannels {
		close(channels.Send)
		close(channels.Recv)
	}
	delete(typeChannels, id)
}

func (c *ServerConn) CloseAll() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, typeChannels := range c.channels {
		for _, idChannels := range typeChannels {
			for _, channels := range idChannels {
				close(channels.Send)
				close(channels.Recv)
			}
		}
	}
	c.channels = nil
}
