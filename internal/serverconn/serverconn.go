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
	"strings"
	"sync"

	"github.com/avast/retry-go"
	"golang.org/x/exp/slog"

	"github.com/dtrust-project/dots-server/internal/config"
)

type Channels struct {
	Send chan any
	Recv chan any
}

type ServerConn struct {
	connections map[string]net.Conn
	encoders    map[string]*gob.Encoder
	decoders    map[string]*gob.Decoder
	comms       map[any]*ServerComm
	mutex       sync.RWMutex
	config      *config.Config
}

func (c *ServerConn) handleIncomingMessage(nodeId string, msg *serverMsg) {
	msgLog := slog.With(
		"otherNodeId", nodeId,
		"commId", msg.CommId,
		"msgTag", msg.Tag,
	)
	msgLog.Debug("Received server message")

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	comm, ok := c.comms[msg.CommId]
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
			c.Unregister(msg.CommId)
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

	// Start listener.
	var listener net.Listener
	if conf.PeerSecurity == config.PeerSecurityTLS {
		serverTlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{conf.PeerTLSCert},
			ClientAuth:         tls.RequireAnyClientCert,
			InsecureSkipVerify: true,
		}

		l, err := tls.Listen("tcp", fmt.Sprintf("%s:%d", conf.PeerBindAddr, conf.PeerPort), serverTlsConfig)
		if err != nil {
			slog.Error("Failed to listen for TLS connections", "err", err)
			return err
		}
		listener = l
	} else {
		l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.PeerBindAddr, conf.PeerPort))
		if err != nil {
			slog.Error("Failed to listen for TCP connecdtions", "err", err)
			return err
		}
		listener = l
	}
	defer listener.Close()

	// Set up pairwise connections with all nodes.
	connChan := make(chan *struct {
		nodeId string
		conn   net.Conn
	})
	errChan := make(chan error)
	for nodeId, nodeConfig := range conf.Nodes {
		if nodeId == conf.OurNodeId {
			continue
		}

		go func(nodeId string, nodeConfig *config.NodeConfig) {
			var conn net.Conn
			var otherId string
			if conf.OurNodeRank > conf.NodeRanks[nodeId] {
				// Act as dialer for lower ranks.
				if err := retry.Do(
					func() error {
						if conf.PeerSecurity == config.PeerSecurityTLS {
							clientTlsConfig := &tls.Config{
								Certificates: []tls.Certificate{conf.PeerTLSCert},
							}
							// If there is no pinned cert, use DNS name
							// authentication. Else, set InsecureSkipVerify and
							// verify the pinned certificate ourselves.
							if nodeConfig.PeerTLSCert != nil {
								clientTlsConfig.InsecureSkipVerify = true
								clientTlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
									if !bytes.Equal(rawCerts[0], nodeConfig.PeerTLSCert.Raw) {
										slog.Warn("Failed to verify pinned peer certificate",
											"nodeId", nodeId,
										)
										return errors.New("Failed to verify pinned peer certificate")
									}
									return nil
								}
							} else {
								clientTlsConfig.ServerName, _, _ = strings.Cut(nodeConfig.Addr, ":")
							}

							c, err := tls.Dial("tcp", nodeConfig.Addr, clientTlsConfig)
							if err != nil {
								return err
							}
							conn = c
						} else {
							c, err := net.Dial("tcp", nodeConfig.Addr)
							if err != nil {
								return err
							}
							conn = c
						}
						return nil
					},
				); err != nil {
					slog.Error("Failed to dial a connection",
						"err", err,
						"nodeId", nodeId,
						"nodeAddr", nodeConfig.Addr,
					)
					errChan <- err
					return
				}

				// Send our ID to the server.
				encoder := gob.NewEncoder(conn)
				if err := encoder.Encode(conf.OurNodeId); err != nil {
					slog.Error("Failed to send our ID to other node")
					errChan <- err
					return
				}

				otherId = nodeId
			} else {
				// Act as listener for higher ranks.
				c, err := listener.Accept()
				if err != nil {
					slog.Error("Failed to accept a connection", "err", err)
					errChan <- err
					return
				}
				conn = c

				// Receive other ID from the other client.
				decoder := gob.NewDecoder(conn)
				if err := decoder.Decode(&otherId); err != nil {
					slog.Error("Failed to receive other ID into our other node")
					errChan <- err
					return
				}

				// Get other node's config.
				otherConfig := conf.Nodes[otherId]
				if otherConfig == nil {
					slog.Error("Other node is not in config",
						"nodeId", otherId,
					)
					errChan <- errors.New("Other node is not in config")
					return
				}

				if conf.PeerSecurity == config.PeerSecurityTLS {
					state := conn.(*tls.Conn).ConnectionState()

					if len(state.PeerCertificates) == 0 {
						slog.Warn("No peer certificate presented",
							"nodeId", otherId,
						)
						errChan <- errors.New("No peer certificate presented")
						return
					}

					if otherConfig.PeerTLSCert != nil {
						// Verify pinned certificate.
						if !bytes.Equal(state.PeerCertificates[0].Raw, otherConfig.PeerTLSCert.Raw) {
							slog.Warn("Failed to verify pinned peer certificate",
								"nodeId", otherId,
							)
							errChan <- errors.New("Failed to verify pinned peer certificate")
							return
						}
					} else {
						// Verify certificate according to roots.

						// Build intermediate pool.
						intermediates := x509.NewCertPool()
						for _, cert := range state.PeerCertificates[1:] {
							intermediates.AddCert(cert)
						}

						// Get domain name.
						hostname, _, _ := strings.Cut(otherConfig.Addr, ":")

						if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
							DNSName:       hostname,
							Intermediates: intermediates,
						}); err != nil {
							slog.Warn("Failed to verify peer certificate",
								"err", err,
								"nodeId", otherId,
							)
							errChan <- err
							return
						}
					}
				}
			}

			// Extract socket and output.
			connChan <- &struct {
				nodeId string
				conn   net.Conn
			}{
				nodeId: otherId,
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
	c.comms = make(map[any]*ServerComm)

	return nil
}

func (c *ServerConn) Register(ctx context.Context, commId any) (*ServerComm, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Ensure comm doesn't already exist.
	if _, ok := c.comms[commId]; ok {
		return nil, errors.New("Channel already exists for message type and ID")
	}

	// Make comm.
	comm := &ServerComm{
		commId:  commId,
		recvBuf: make(map[string]map[any]chan any),
		conn:    c,
		logger: slog.With(
			"commId", commId,
		),
	}
	for nodeId := range c.config.Nodes {
		comm.recvBuf[nodeId] = make(map[any]chan any)
	}

	c.comms[commId] = comm

	return comm, nil
}

func (c *ServerConn) Unregister(commId any) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	comm, ok := c.comms[commId]
	if !ok {
		return
	}
	for _, nodeChans := range comm.recvBuf {
		for _, c := range nodeChans {
			close(c)
		}
	}
	delete(c.comms, commId)
}

func (c *ServerConn) CloseAll() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, comm := range c.comms {
		for _, nodeChans := range comm.recvBuf {
			for _, c := range nodeChans {
				close(c)
			}
		}
	}
	c.comms = nil
}
