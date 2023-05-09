package dotsservergrpc

import (
	"context"
	"encoding/gob"
	"sync"

	"github.com/dtrust-project/dotspb/go/dotspb"
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

	apps      map[string]*appinstance.AppInstance
	appsMutex sync.RWMutex

	dotspb.UnimplementedDecExecServer
}

// Assert DotsServerGrpc fulfills dotspb.DecExecServer.
var _ dotspb.DecExecServer = (*DotsServerGrpc)(nil)

type platformCommId struct{}

type appCommId struct {
	AppName string
}

func init() {
	gob.Register(appCommId{})
}

func (server *DotsServerGrpc) spawnInstance(conf *config.Config, appName string, appComm *serverconn.ServerComm) error {
	appConfig := conf.Apps[appName]

	appLog := slog.With(
		"appName", appName,
		"appPath", appConfig.Path,
	)

	server.appsMutex.Lock()
	defer server.appsMutex.Unlock()

	instance, err := appinstance.Spawn(conf, appConfig.Path, appName, appComm)
	if err != nil {
		appLog.Error("Failed to construct application instance", "err", err)
		return err
	}

	server.apps[appName] = instance

	// Spawn watchdog goroutine to restart app as needed.
	go func() {
		if err := instance.Wait(); err != nil {
			appLog.Warn("Application exited with error", "err", err)
		} else {
			appLog.Warn("Application exited without error", "err", err)
		}
		appLog.Warn("Restarting application instance")

		if err := server.spawnInstance(conf, appName, appComm); err != nil {
			appLog.Error("Failed to restart application instance", "err", err)
			return
		}
	}()

	return nil
}

func NewDotsServerGrpc(nodeId string, conf *config.Config) (*DotsServerGrpc, error) {
	server := &DotsServerGrpc{
		config: conf,

		apps: make(map[string]*appinstance.AppInstance),
	}

	// Establish server-to-server connection.
	if err := server.conns.Establish(conf); err != nil {
		slog.Error("Failed to establish server-to-server connection", "err", err)
		return nil, err
	}

	// Establish a communicator for control messages.
	controlComm, err := server.conns.Register(context.Background(), platformCommId{})
	if err != nil {
		slog.Error("Failed to establish server control communicator", "err", err)
		return nil, err
	}
	server.controlComm = controlComm

	slog.Info("Server connections established")

	// Spawn application instances.
	for appName := range conf.Apps {
		appComm, err := server.conns.Register(context.Background(), appCommId{appName})
		if err != nil {
			slog.Error("Failed to establish application communicator", "err", err)
			return nil, err
		}

		if err := server.spawnInstance(conf, appName, appComm); err != nil {
			slog.Error("Failed to construct application instance", "err", err)
			return nil, err
		}
	}

	slog.Info("Spawned application instances")

	return server, nil
}

func (s *DotsServerGrpc) Shutdown() {
	s.conns.CloseAll()
}
