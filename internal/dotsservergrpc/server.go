package dotsservergrpc

import (
	"context"

	"github.com/dtrust-project/dotspb/go/dotspb"
	"github.com/google/uuid"
	"golang.org/x/exp/slog"

	"github.com/dtrust-project/dots-server/internal/appinstance"
	"github.com/dtrust-project/dots-server/internal/config"
	"github.com/dtrust-project/dots-server/internal/serverconn"
)

const internalErrMsg = "Internal error"

type DotsServerGrpc struct {
	conns       serverconn.ServerConn
	controlComm *serverconn.ServerComm
	config      *config.Config

	apps map[string]*appinstance.AppInstance

	dotspb.UnimplementedDecExecServer
}

// Assert DotsServerGrpc fulfills dotspb.DecExecServer.
var _ dotspb.DecExecServer = (*DotsServerGrpc)(nil)

func NewDotsServerGrpc(nodeId string, config *config.Config) (*DotsServerGrpc, error) {
	server := &DotsServerGrpc{
		config: config,

		apps: make(map[string]*appinstance.AppInstance),
	}

	// Establish server-to-server connection.
	if err := server.conns.Establish(config); err != nil {
		slog.Error("Failed to establish server-to-server connection", "err", err)
		return nil, err
	}

	// Establish a communicator for control messages.
	controlComm, err := server.conns.Register(context.Background(), serverconn.MsgTypePlatform, uuid.Nil)
	if err != nil {
		slog.Error("Failed to establish server control communicator", "err", err)
		return nil, err
	}
	server.controlComm = controlComm

	slog.Info("Server connections established")

	// Spawn application instances.
	for appName, appConfig := range config.Apps {
		appComm, err := server.conns.Register(context.Background(), serverconn.MsgTypeAppInstance, uuid.Nil)
		if err != nil {
			slog.Error("Failed to establish application communicator", "err", err)
			return nil, err
		}

		instance, err := appinstance.Spawn(config, appConfig.Path, appName, appComm)
		if err != nil {
			slog.Error("Failed to construct application instance", "err", err,
				"appName", appName,
				"appPath", appConfig.Path,
			)
			return nil, err
		}

		server.apps[appName] = instance
	}

	slog.Info("Spawned application instances")

	return server, nil
}

func (s *DotsServerGrpc) Shutdown() {
	s.conns.CloseAll()
}
