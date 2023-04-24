package appinstance

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"sync"
	"syscall"

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
	controlSocket          *os.File
	controlSocketSendMutex sync.Mutex
	controlSocketRecvMutex sync.Mutex

	done chan error
}

type appEnvHeader struct {
	WorldRank         uint32
	WorldSize         uint32
	InputFilesOffset  uint32
	InputFilesCount   uint32
	OutputFilesOffset uint32
	OutputFilesCount  uint32
	FuncNameOffset    uint32
	ControlSocket     uint32
	ArgsOffset        uint32
	ArgsCount         uint32
	_                 [88]byte
}

type appEnvIovec struct {
	Offset uint32
	Length uint32
}

func init() {
	if binary.Size(&appEnvHeader{}) != 128 {
		panic("App environment header must be 128 bytes long")
	}
}

func (instance *AppInstance) execute(ctx context.Context, appPath string, appName string, funcName string, inputFiles []*os.File, outputFiles []*os.File, args [][]byte) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Open and manage control socket.
	controlSocketPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to open control socketpair", "err", err)
		instance.done <- err
		return
	}
	controlSocket := os.NewFile(uintptr(controlSocketPair[0]), "")
	controlSocketApp := os.NewFile(uintptr(controlSocketPair[1]), "")
	defer controlSocketApp.Close()
	defer cancel() // Defer the context cancel only after controlSocket.Close() to prevent error.
	go func() {
		defer controlSocket.Close()
		instance.manageControlSocket(ctx, controlSocket)
	}()

	// Run program.
	cmd := exec.CommandContext(ctx, appPath)
	cmd.ExtraFiles = append(cmd.ExtraFiles, controlSocketApp)
	cmd.ExtraFiles = append(cmd.ExtraFiles, inputFiles...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, outputFiles...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error opening application stdin pipe", "err", err)
		instance.done <- err
		return
	}
	if err := cmd.Start(); err != nil {
		util.LoggerFromContext(ctx).Error("Error starting application", "err", err)
		instance.done <- grpc.Errorf(codes.Internal, "Application error")
		return
	}
	defer cmd.Process.Kill()

	// Generate input for DoTS app environment. Per https://pkg.go.dev/os/exec,
	// the i'th entry of cmd.ExtraFiles is mapped to FD 3 + i, so the loop
	// relies only on the lengths inputFiles, outputFiles, and sockets.
	// Fixed header inputs.
	var envHeader appEnvHeader
	envOffset := uint32(0)
	envHeader.WorldRank = uint32(instance.config.OurNodeRank)
	envHeader.WorldSize = uint32(len(instance.config.Nodes))
	envHeader.ControlSocket = uint32(3)
	envOffset += uint32(binary.Size(&envHeader))
	envInput := make([]byte, envOffset)

	// Input files.
	envHeader.InputFilesOffset = envOffset
	envHeader.InputFilesCount = uint32(len(inputFiles))
	for i := range inputFiles {
		envInput = binary.BigEndian.AppendUint32(envInput, uint32(3+1+i))
	}
	envOffset += uint32(len(inputFiles) * binary.Size(uint32(0)))

	// Ouput files.
	envHeader.OutputFilesOffset = envOffset
	envHeader.OutputFilesCount = uint32(len(outputFiles))
	for i := range outputFiles {
		envInput = binary.BigEndian.AppendUint32(envInput, uint32(3+1+len(inputFiles)+i))
	}
	envOffset += uint32(len(outputFiles) * binary.Size(uint32(0)))

	// Function name.
	envHeader.FuncNameOffset = envOffset
	envInput = append(envInput, []byte(funcName)...)
	envInput = append(envInput, 0)
	envOffset += uint32(len(funcName) + 1)

	// Exec arguments.
	envHeader.ArgsOffset = envOffset
	envHeader.ArgsCount = uint32(len(args))
	envArgOffset := uint32(len(args) * binary.Size(&appEnvIovec{}))
	envArgInputBuf := new(bytes.Buffer)
	for _, arg := range args {
		envArgIovec := appEnvIovec{
			Offset: envOffset + envArgOffset,
			Length: uint32(len(arg)),
		}
		if err := binary.Write(envArgInputBuf, binary.BigEndian, &envArgIovec); err != nil {
			util.LoggerFromContext(ctx).Error("Failed to marshal app environment arg iovec", "err", err)
			instance.done <- err
			return
		}
		envArgOffset += uint32(len(arg) + 1)
	}
	envInput = append(envInput, envArgInputBuf.Bytes()...)
	for _, arg := range args {
		envInput = append(envInput, arg...)
		envInput = append(envInput, 0)
	}
	envOffset += envArgOffset

	// Marshal the header and place it at the beginning of the input slice.
	envInputBuf := new(bytes.Buffer)
	if err := binary.Write(envInputBuf, binary.BigEndian, &envHeader); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to marshal app environment header", "err", err)
		instance.done <- err
		return
	}
	copy(envInput, envInputBuf.Bytes())

	// Write environment length and environment to stdin.
	if err := binary.Write(stdin, binary.BigEndian, envOffset); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to write environment length to application stdin", "err", err)
		instance.done <- err
		return
	}
	if _, err := stdin.Write(envInput); err != nil {
		util.LoggerFromContext(ctx).Error("Failed to write environment to application stdin", "err", err)
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

func ExecApp(ctx context.Context, conf *config.Config, appPath string, appName string, funcName string, inputFiles []*os.File, outputFiles []*os.File, args [][]byte, serverComm *serverconn.ServerComm) (*AppInstance, error) {
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
	go instance.execute(ctx, appPath, appName, funcName, inputFiles, outputFiles, args)
	return instance, nil
}
