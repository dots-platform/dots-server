package appinstance

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/internal/serverconn"
	"github.com/dtrust-project/dtrust-server/internal/util"
)

type AppInstance struct {
	appName                string
	funcName               string
	config                 *config.Config
	serverComm             *serverconn.ServerComm
	bufferedMsgs           map[int]map[int][]byte
	controlSocket          *net.UnixConn
	controlSocketSendMutex sync.Mutex
	controlSocketRecvMutex sync.Mutex

	done chan error
}

func (instance *AppInstance) execute(ctx context.Context, appPath string, appName string, funcName string, inputFiles []*os.File, outputFiles []*os.File) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Run program.
	cmd := exec.CommandContext(ctx, appPath)
	cmd.ExtraFiles = append(cmd.ExtraFiles, inputFiles...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, outputFiles...)
	stdin, err := cmd.StdinPipe()
	if err := cmd.Start(); err != nil {
		util.LoggerFromContext(ctx).Error("Error starting application", "err", err)
		instance.done <- grpc.Errorf(codes.Internal, "Application error")
		return
	}
	defer cmd.Process.Kill()

	// Open control socket.
	var listenConfig net.ListenConfig
	controlSocketPath := fmt.Sprintf("/tmp/socket-%d", cmd.Process.Pid)
	os.Remove(controlSocketPath)
	controlConn, err := listenConfig.Listen(ctx, "unix", controlSocketPath)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error creating control socket listener", "err", err)
		instance.done <- grpc.Errorf(codes.Internal, "Application error")
		return
	}
	defer os.Remove(controlSocketPath)
	defer controlConn.Close()
	defer cancel() // Defer the context cancel only after controlConn.Close() to prevent error.

	// Accept connection on control socket.
	go func() {
		controlSocket, err := controlConn.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				util.LoggerFromContext(ctx).Error("Error accepting control socket connection", "err", err)
				return
			}
		}
		defer controlSocket.Close()
		instance.manageControlSocket(ctx, controlSocket.(*net.UnixConn))
	}()

	// Generate input for DoTS app environment. Per https://pkg.go.dev/os/exec,
	// the i'th entry of cmd.ExtraFiles is mapped to FD 3 + i, so the loop
	// relies only on the lengths inputFiles, outputFiles, and sockets.
	dotsEnvInput := ""
	dotsEnvInput += strconv.Itoa(instance.config.OurNodeRank) + "\n"
	dotsEnvInput += strconv.Itoa(len(instance.config.Nodes)) + "\n"
	for i := range inputFiles {
		if i > 0 {
			dotsEnvInput += " "
		}
		dotsEnvInput += strconv.Itoa(3 + i)
	}
	dotsEnvInput += "\n"
	for i := range inputFiles {
		if i > 0 {
			dotsEnvInput += " "
		}
		dotsEnvInput += strconv.Itoa(3 + len(inputFiles) + i)
	}
	dotsEnvInput += "\n"
	dotsEnvInput += funcName + "\n"

	// Write environment to stdin.
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error opening application stdin pipe", "err", err)
		instance.done <- err
		return
	}
	if _, err := stdin.Write([]byte(dotsEnvInput)); err != nil {
		util.LoggerFromContext(ctx).Error("Error writing environment to application stdin", "err", err)
		instance.done <- err
		return
	}

	// Wait for application to finish.
	if err := cmd.Wait(); err != nil {
		util.LoggerFromContext(ctx).Warn("Application exited with non-zero return code", "err", err)
		instance.done <- err
		return
	}

	instance.done <- nil
}

func (instance *AppInstance) Wait() error {
	return <-instance.done
}

func ExecApp(ctx context.Context, conf *config.Config, appPath string, appName string, funcName string, inputFiles []*os.File, outputFiles []*os.File, serverComm *serverconn.ServerComm) (*AppInstance, error) {
	ctx = util.ContextWithLogger(ctx, slog.With(
		"appName", appName,
		"appFuncName", funcName,
	))

	instance := &AppInstance{
		appName:    appName,
		funcName:   funcName,
		config:     conf,
		serverComm: serverComm,
		done:       make(chan error),
	}
	go instance.execute(ctx, appPath, appName, funcName, inputFiles, outputFiles)
	return instance, nil
}
