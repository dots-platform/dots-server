package serverconn

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
	"golang.org/x/exp/slog"

	"github.com/dtrust-project/dots-server/internal/config"
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
		return fmt.Sprintf("INVALID: 0x%04x", uint16(t))
	}
}

type Channels struct {
	Send chan any
	Recv chan any
}

type ServerConn struct {
	connections map[string]net.Conn
	encoders    map[string]*gob.Encoder
	decoders    map[string]*gob.Decoder
	comms       map[MsgType]map[uuid.UUID]*ServerComm
	mutex       sync.RWMutex
	config      *config.Config
}

func (c *ServerConn) handleIncomingMessage(nodeId string, msg *serverMsg) {
	msgLog := slog.With(
		"otherNodeId", nodeId,
		"msgType", msg.Type,
		"msgSubtypeId", msg.SubtypeId,
	)
	msgLog.Debug("Received server message")

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	typeComms, ok := c.comms[msg.Type]
	if !ok {
		msgLog.Warn("Received message with no listener")
		return
	}

	comm, ok := typeComms[msg.SubtypeId]
	if !ok {
		msgLog.Warn("Received message with no listener")
		return
	}

	var recvChan chan any
	func() {
		comm.recvMutex.Lock()
		defer comm.recvMutex.Unlock()

		r, ok := comm.recvBuf[nodeId][msg.Tag]
		if !ok {
			r = make(chan any, recvBufSize)
			comm.recvBuf[nodeId][msg.Tag] = r
		}
		recvChan = r
	}()

	select {
	case recvChan <- msg.Data:
	default:
		msgLog.Warn("Listener receive buffer full; tearing down the connection")
		func() {
			c.mutex.RUnlock()
			defer c.mutex.RLock()
			c.Unregister(msg.Type, msg.SubtypeId)
		}()
	}
}

func (c *ServerConn) receiveMessages(nodeId string, decoder *gob.Decoder) {
	connLog := slog.With(
		"otherNodeId", nodeId,
	)

	// Repeatedly receive messages and forward them to the approprate registered
	// channel.
	loop := true
	for loop {
		var serverMsg serverMsg
		if err := decoder.Decode(&serverMsg); err != nil {
			connLog.Error("Error reading from server connection", "err", err)
			loop = false
			break
		}
		c.handleIncomingMessage(nodeId, &serverMsg)
	}
}

func (c *ServerConn) Establish(conf *config.Config) error {
	c.config = conf

	// Set up pairwise TCP connections with all nodes.
	connChan := make(chan *struct {
		nodeId string
		conn   net.Conn
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

			var tlsConfig *tls.Config
			if conf.PeerSecurity == config.PeerSecurityTLS {
				// TODO Add server name to config somehow to authenticate server
				// name as part of TLS connection.
				tlsConfig = &tls.Config{
					Certificates: []tls.Certificate{conf.OurNodeConfig.PeerTLSCertTLS},
					ClientAuth:   tls.RequireAndVerifyClientCert,
				}
				if nodeConfig.PeerTLSCertX509 != nil {
					tlsConfig.InsecureSkipVerify = true
					tlsConfig.ClientAuth = tls.RequireAnyClientCert
					tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
						if !bytes.Equal(rawCerts[0], nodeConfig.PeerTLSCertX509.Raw) {
							return errors.New("Peer certificate does not match pinned certificate")
						}
						return nil
					}
				}
			}

			var conn net.Conn
			if ourRank < otherRank {
				// Act as the listener for higher ranks.
				var listener net.Listener
				if conf.PeerSecurity == config.PeerSecurityTLS {
					l, err := tls.Listen("tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]), tlsConfig)
					if err != nil {
						errChan <- err
						return
					}
					listener = l
				} else {
					l, err := net.Listen("tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
					if err != nil {
						errChan <- err
						return
					}
					listener = l
				}
				defer listener.Close()
				c, err := listener.Accept()
				if err != nil {
					errChan <- err
					return
				}
				conn = c
			} else {
				// Act as dialer for lower ranks.
				if err := retry.Do(
					func() error {
						if conf.PeerSecurity == config.PeerSecurityTLS {
							c, err := tls.Dial("tcp", fmt.Sprintf(":%d", nodeConfig.Ports[ourRank]), tlsConfig)
							if err != nil {
								return err
							}
							conn = c
						} else {
							c, err := net.Dial("tcp", fmt.Sprintf(":%d", nodeConfig.Ports[ourRank]))
							if err != nil {
								return err
							}
							conn = c
						}
						return nil
					},
				); err != nil {
					errChan <- err
				}
			}

			// Extract socket and output.
			connChan <- &struct {
				nodeId string
				conn   net.Conn
			}{
				nodeId: nodeId,
				conn:   conn,
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
			slog.Error("Error opening server-to-server socket", "err", err)
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
	c.comms = make(map[MsgType]map[uuid.UUID]*ServerComm)

	return nil
}

func (c *ServerConn) Register(ctx context.Context, msgType MsgType, id uuid.UUID) (*ServerComm, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Ensure comm doesn't already exist.
	typeComms, ok := c.comms[msgType]
	if !ok {
		typeComms = make(map[uuid.UUID]*ServerComm)
		c.comms[msgType] = typeComms
	}
	if _, ok := typeComms[id]; ok {
		return nil, errors.New("Channel already exists for message type and ID")
	}

	// Make comm.
	comm := &ServerComm{
		msgType:   msgType,
		subtypeId: id,
		recvBuf:   make(map[string]map[any]chan any),
		conn:      c,
		logger: slog.With(
			"msgType", msgType,
			"msgSubtypeId", id,
		),
	}
	for nodeId := range c.config.Nodes {
		comm.recvBuf[nodeId] = make(map[any]chan any)
	}

	typeComms[id] = comm

	return comm, nil
}

func (c *ServerConn) Unregister(msgType MsgType, id uuid.UUID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	typeComms, ok := c.comms[msgType]
	if !ok {
		return
	}
	comm, ok := typeComms[id]
	if !ok {
		return
	}
	for _, nodeChans := range comm.recvBuf {
		for _, c := range nodeChans {
			close(c)
		}
	}
	delete(typeComms, id)
}

func (c *ServerConn) CloseAll() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, typeComms := range c.comms {
		for _, comm := range typeComms {
			for _, nodeChans := range comm.recvBuf {
				for _, c := range nodeChans {
					close(c)
				}
			}
		}
	}
	c.comms = nil
}
