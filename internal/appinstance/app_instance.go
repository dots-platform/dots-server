package appinstance

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"golang.org/x/exp/slog"

	"github.com/dtrust-project/dots-server/internal/config"
	"github.com/dtrust-project/dots-server/internal/serverconn"
	"github.com/dtrust-project/dots-server/internal/util"
)

type requestStatus int

const (
	requestStatusPending  requestStatus = 1
	requestStatusAccepted               = 2
)

type AppInstance struct {
	AppName string
	AppPath string

	config     *config.Config
	serverComm *serverconn.ServerComm

	controlSocket          *os.File
	controlSocketSendMutex sync.Mutex
	controlSocketRecvMutex sync.Mutex

	requests        map[uuid.UUID]*AppRequest
	pendingRequests chan *AppRequest
	requestsMutex   sync.RWMutex
}

const pendingRequestsBufLen = 100

func Spawn(conf *config.Config, appPath string, appName string, serverComm *serverconn.ServerComm) (*AppInstance, error) {
	ctx := util.ContextWithLogger(context.Background(), slog.With(
		"appName", appName,
		"appPath", appPath,
	))

	// Open control socket.
	controlSocketPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to open control socketpair", "err", err)
		return nil, err
	}
	controlSocket := os.NewFile(uintptr(controlSocketPair[0]), "")
	controlSocketApp := os.NewFile(uintptr(controlSocketPair[1]), "")
	defer controlSocketApp.Close()

	// Run program.
	cmd := exec.CommandContext(ctx, appPath)
	cmd.ExtraFiles = append(cmd.ExtraFiles, controlSocketApp)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error opening application stdin pipe", "err", err)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		util.LoggerFromContext(ctx).Error("Error starting application", "err", err)
		return nil, err
	}
	controlSocketApp.Close()

	instance := &AppInstance{
		AppName: appName,
		AppPath: appPath,

		config:     conf,
		serverComm: serverComm,

		controlSocket: controlSocket,

		requests:        make(map[uuid.UUID]*AppRequest),
		pendingRequests: make(chan *AppRequest, pendingRequestsBufLen),
	}

	// Manage control socket.
	go func() {
		defer controlSocket.Close()
		instance.manageControlSocket(ctx)
	}()

	return instance, nil
}

func (instance *AppInstance) NewRequest(ctx context.Context, requestId uuid.UUID, funcName string, args [][]byte) (*AppRequest, error) {
	instance.requestsMutex.Lock()
	defer instance.requestsMutex.Unlock()

	_, ok := instance.requests[requestId]
	if ok {
		return nil, errors.New("Request already exists for app instance")
	}

	req := &AppRequest{
		Id:       requestId,
		FuncName: funcName,
		Args:     args,

		ctx:      ctx,
		instance: instance,
		done:     make(chan appResult),
	}

	instance.requests[requestId] = req
	instance.pendingRequests <- req

	return req, nil
}
