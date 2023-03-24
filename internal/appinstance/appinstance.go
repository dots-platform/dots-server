package appinstance

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"

	"github.com/dtrust-project/dtrust-server/internal/config"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type AppInstance struct {
	appName  string
	funcName string
	appLog   log.FieldLogger
	config   *config.Config

	ctx    context.Context
	cancel context.CancelFunc

	done chan error
}

func (instance *AppInstance) execute(ctx context.Context, appPath string, appName string, funcName string, inputFiles []*os.File, outputFiles []*os.File, sockets []*os.File) {
	// Run program.
	cmd := exec.CommandContext(ctx, appPath)
	cmd.ExtraFiles = append(cmd.ExtraFiles, inputFiles...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, outputFiles...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, sockets...)
	stdin, err := cmd.StdinPipe()
	if err := cmd.Start(); err != nil {
		instance.appLog.WithError(err).Error("Error starting application")
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
		instance.appLog.WithError(err).Error("Error creating control socket listener")
		instance.done <- grpc.Errorf(codes.Internal, "Application error")
		return
	}
	defer os.Remove(controlSocketPath)
	defer controlConn.Close()
	defer instance.cancel() // Defer the conctext cancel only after controlConn.Close() to prevent error.

	// Accept connection on control socket.
	go func() {
		controlSocket, err := controlConn.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				instance.appLog.WithError(err).Error("Error accepting control socket connection")
				return
			}
		}
		defer controlSocket.Close()
		instance.manageControlSocket(ctx, appName, funcName, controlSocket.(*net.UnixConn))
	}()

	// Generate input for DoTS app environment. Per https://pkg.go.dev/os/exec,
	// the i'th entry of cmd.ExtraFiles is mapped to FD 3 + i, so the loop
	// relies only on the lengths inputFiles, outputFiles, and sockets.
	dotsEnvInput := ""
	dotsEnvInput += strconv.Itoa(instance.config.OurNodeRank) + "\n"
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
	for i, socket := range sockets {
		if i > 0 {
			dotsEnvInput += " "
		}
		if socket == nil {
			dotsEnvInput += "0"
		} else {
			dotsEnvInput += strconv.Itoa(3 + len(inputFiles) + len(outputFiles) + i)
		}
	}
	dotsEnvInput += "\n"
	dotsEnvInput += funcName + "\n"

	// Write environment to stdin.
	if err != nil {
		instance.appLog.WithError(err).Error("Error opening application stdin pipe")
		instance.done <- err
		return
	}
	if _, err := stdin.Write([]byte(dotsEnvInput)); err != nil {
		instance.appLog.WithError(err).Error("Error writing environment to application stdin")
		instance.done <- err
		return
	}

	// Wait for application to finish.
	if err := cmd.Wait(); err != nil {
		instance.appLog.WithError(err).Warn("Application exited with non-zero return code")
		instance.done <- err
		return
	}

	instance.done <- nil
}

func (instance *AppInstance) Wait() error {
	return <-instance.done
}

func ExecApp(ctx context.Context, conf *config.Config, appPath string, appName string, funcName string, inputFiles []*os.File, outputFiles []*os.File, sockets []*os.File) (*AppInstance, error) {
	ctx, cancel := context.WithCancel(ctx)
	instance := &AppInstance{
		appName:  appName,
		funcName: funcName,
		appLog: log.WithFields(log.Fields{
			"appName":     appName,
			"appFuncName": funcName,
		}),
		config: conf,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan error),
	}
	go instance.execute(ctx, appPath, appName, funcName, inputFiles, outputFiles, sockets)
	return instance, nil
}