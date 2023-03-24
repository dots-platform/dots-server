package dotsservergrpc

import (
	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/internal/serverconn"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
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
	return server, nil
}
