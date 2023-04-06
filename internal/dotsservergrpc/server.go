package dotsservergrpc

import (
	"context"

	"github.com/google/uuid"

	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/internal/serverconn"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
	log "github.com/sirupsen/logrus"
)

const internalErrMsg = "Internal error"

type DotsServerGrpc struct {
	conns       serverconn.ServerConn
	controlComm *serverconn.ServerComm
	config      *config.Config

	dotspb.UnimplementedDecExecServer
}

// Assert DotsServerGrpc fulfills dotspb.DecExecServer.
var _ dotspb.DecExecServer = (*DotsServerGrpc)(nil)

func NewDotsServerGrpc(nodeId string, config *config.Config) (*DotsServerGrpc, error) {
	server := &DotsServerGrpc{
		config: config,
	}

	// Establish server-to-server connection.
	if err := server.conns.Establish(context.Background(), config); err != nil {
		log.Error("Failed to establish server-to-server connection")
		return nil, err
	}

	// Establish a communicator for control messages.
	controlComm, err := server.conns.Register(context.Background(), serverconn.MsgTypePlatform, uuid.Nil)
	if err != nil {
		log.Error("Failed to establish server control communicator")
		return nil, err
	}
	server.controlComm = controlComm

	log.Info("Server connections established")

	return server, nil
}

func (s *DotsServerGrpc) Shutdown() {
	s.conns.CloseAll()
}
