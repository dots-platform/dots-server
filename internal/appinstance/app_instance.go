package appinstance

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"sync"
	"sync/atomic"
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

	cmd                    *exec.Cmd
	controlSocket          *os.File
	controlSocketSendMutex sync.Mutex
	controlSocketRecvMutex sync.Mutex
	controlSocketMsgCounter atomic.Uint64

	requests        map[uuid.UUID]*AppRequest
	pendingRequests chan *AppRequest
	requestsMutex   sync.RWMutex
}

const pendingRequestsBufLen = 100

type appEnvInput struct {
	ControlSocket uint32
	WorldRank     uint32
	WorldSize     uint32
	_             [116]byte
}

func init() {
	if binary.Size(&appEnvInput{}) != 128 {
		panic("App environment input must be 128 bytes long")
	}
}

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

	// Get absolute app path.
	if appPath[0] != '/' {
		workdir, err := os.Getwd()
		if err != nil {
			util.LoggerFromContext(ctx).Error("Failed to get current working directory", "err", err)
			return nil, err
		}
		appPath = path.Clean(path.Join(workdir, appPath))
	}

	// Run program.
	cmd := exec.CommandContext(ctx, appPath)
	cmd.Dir = path.Join(conf.FileStorageDir, conf.OurNodeId)
	os.MkdirAll(cmd.Dir, 0o755)
	cmd.ExtraFiles = append(cmd.ExtraFiles, controlSocketApp)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to open application stdin pipe", "err", err)
		return nil, err
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to open application stdout pipe", "err", err)
		return nil, err
	}
	go func() {
		defer stdout.Close()
		reader := bufio.NewReader(stdout)
		for {
			line, _, err := reader.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				util.LoggerFromContext(ctx).Error("Failed reading application stdout", "err", err)
				break
			}
			fmt.Printf("[%s stdout] %s\n", appName, line)
		}
	}()
	stderr, err := cmd.StderrPipe()
	go func() {
		defer stderr.Close()
		reader := bufio.NewReader(stderr)
		for {
			line, _, err := reader.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				util.LoggerFromContext(ctx).Error("Failed reading application stderr", "err", err)
				break
			}
			fmt.Printf("[%s stderr] %s\n", appName, line)
		}
	}()
	if err := cmd.Start(); err != nil {
		util.LoggerFromContext(ctx).Error("Error starting application", "err", err)
		return nil, err
	}
	controlSocketApp.Close()

	// Write startup environment to application stdin.
	appEnv := appEnvInput{
		ControlSocket: 3,
		WorldRank:     uint32(conf.OurNodeRank),
		WorldSize:     uint32(len(conf.Nodes)),
	}
	if err := binary.Write(stdin, binary.BigEndian, &appEnv); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to write startup environment to application stdin", "err", err)
	}

	instance := &AppInstance{
		AppName: appName,
		AppPath: appPath,

		config:     conf,
		serverComm: serverComm,

		cmd:           cmd,
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

func (instance *AppInstance) Wait() error {
	return instance.cmd.Wait()
}
