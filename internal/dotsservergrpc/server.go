package dotsservergrpc

import (
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
)

type DotsServerGrpc struct {
	dotspb.UnimplementedDecExecServer
}

// Assert DotsServerGrpc fulfills dotspb.DecExecServer.
var _ dotspb.DecExecServer = (*DotsServerGrpc)(nil)
