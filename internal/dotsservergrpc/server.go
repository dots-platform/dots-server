package dotsservergrpc

import (
	"context"

	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/internal/serverconn"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
	log "github.com/sirupsen/logrus"
)

const internalErrMsg = "Internal error"

type DotsServerGrpc struct {
	conns  serverconn.ServerConn
	config *config.Config

	dotspb.UnimplementedDecExecServer
}

// Assert DotsServerGrpc fulfills dotspb.DecExecServer.
var _ dotspb.DecExecServer = (*DotsServerGrpc)(nil)

func NewDotsServerGrpc(nodeId string, config *config.Config) (*DotsServerGrpc, error) {
	server := &DotsServerGrpc{
		config: config,
	}

	if err := server.conns.Establish(context.Background(), config); err != nil {
		return nil, err
	}

	log.Info("Server connections established")

	return server, nil
}

func (s *DotsServerGrpc) Shutdown() {
	s.conns.CloseAll()
}
