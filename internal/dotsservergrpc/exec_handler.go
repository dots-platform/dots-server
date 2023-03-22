package dotsservergrpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"

	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
)

func (s *DotsServerGrpc) Exec(ctx context.Context, app *dotspb.App) (*dotspb.Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Look up app config.
	appConfig, ok := s.config.Apps[app.GetAppName()]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "App with name not found")
	}

	execLog := log.WithFields(log.Fields{
		"appName":     app.GetAppName(),
		"appFuncName": app.GetFuncName(),
	})

	// Set up pairwise TCP connections with all apps.
	var dialer net.Dialer
	var listenConfig net.ListenConfig
	socketChan := make(chan *struct {
		rank int
		conn *net.TCPConn
	})
	errChan := make(chan error)
	ourRank := s.config.NodeRanks[s.nodeId]
	ourConfig := s.config.Nodes[s.nodeId]
	for nodeId, nodeConfig := range s.config.Nodes {
		go func(nodeId string, nodeConfig *config.NodeConfig) {
			otherRank := s.config.NodeRanks[nodeId]
			if ourRank == otherRank {
				return
			}

			var conn net.Conn
			if ourRank < otherRank {
				// Act as the listener for higher ranks.
				listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", ourConfig.Ports[otherRank]))
				if err != nil {
					errChan <- err
					return
				}
				defer listener.Close()
				conn, err = listener.Accept()
				if err != nil {
					errChan <- err
					return
				}
			} else {
				// Act as dialer for lower ranks.
				if err := retry.Do(
					func() error {
						c, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf(":%d", nodeConfig.Ports[ourRank]))
						if err != nil {
							return err
						}
						conn = c
						return nil
					},
					retry.Context(ctx),
				); err != nil {
					errChan <- err
				}
			}

			// Extract socket and output.
			socketChan <- &struct {
				rank int
				conn *net.TCPConn
			}{
				rank: otherRank,
				conn: conn.(*net.TCPConn),
			}
		}(nodeId, nodeConfig)
	}
	sockets := make([]*os.File, len(s.config.Nodes))
	var loopErr error
	for i := 0; i < len(s.config.Nodes)-1; i++ {
		select {
		case s := <-socketChan:
			defer s.conn.Close()
			socket, err := s.conn.File()
			if err != nil {
				loopErr = err
				continue
			}
			defer socket.Close()
			sockets[s.rank] = socket
		case err := <-errChan:
			loopErr = err
			execLog.WithFields(log.Fields{
				"err": err,
			}).Error("Error opening application socket")
		}
	}
	if loopErr != nil {
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	// Open input files.
	inputFiles := make([]*os.File, len(app.GetInFiles()))
	for i, inputName := range app.GetInFiles() {
		inputPath := path.Join(s.config.FileStorageDir, s.nodeId, app.GetClientId(), inputName)
		inputFile, err := os.Open(inputPath)
		if err != nil {
			if !os.IsNotExist(err) {
				execLog.WithFields(log.Fields{
					"blobPath": inputPath,
					"err":      err,
				}).Error("Error opening input file")
				return nil, grpc.Errorf(codes.Internal, internalErrMsg)
			}

			// Handle not found error.
			return nil, grpc.Errorf(codes.NotFound, "Blob with key not found")
		}
		defer inputFile.Close()
		inputFiles[i] = inputFile
	}

	// Open output files.
	outputFiles := make([]*os.File, len(app.GetOutFiles()))
	for i, outputName := range app.GetOutFiles() {
		outputPath := path.Join(s.config.FileStorageDir, s.nodeId, app.GetClientId(), outputName)
		outputFile, err := os.Create(outputPath)
		if err != nil {
			if !os.IsNotExist(err) {
				execLog.WithFields(log.Fields{
					"blobPath": outputPath,
					"err":      err,
				}).Error("Error opening output file")
				return nil, grpc.Errorf(codes.Internal, internalErrMsg)
			}

			// Handle not found error.
			return nil, grpc.Errorf(codes.NotFound, "Blob with key not found")
		}
		defer outputFile.Close()
		outputFiles[i] = outputFile
	}

	// Run program.
	cmd := exec.CommandContext(ctx, appConfig.Path)
	cmd.ExtraFiles = append(cmd.ExtraFiles, inputFiles...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, outputFiles...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, sockets...)
	stdin, err := cmd.StdinPipe()
	if err := cmd.Start(); err != nil {
		execLog.WithFields(log.Fields{
			"err": err,
		}).Error("Error starting application")
		return nil, grpc.Errorf(codes.Internal, "Application error")
	}
	defer cmd.Process.Kill()

	// Open control socket.
	controlSocketPath := fmt.Sprintf("/tmp/socket-%d", cmd.Process.Pid)
	os.Remove(controlSocketPath)
	controlConn, err := listenConfig.Listen(ctx, "unix", controlSocketPath)
	if err != nil {
		execLog.WithFields(log.Fields{
			"err": err,
		}).Error("Error creating control socket listener")
		return nil, grpc.Errorf(codes.Internal, "Application error")
	}
	defer os.Remove(controlSocketPath)
	defer controlConn.Close()
	defer cancel() // Cancel again to make sure it's called before controlConn.Close()

	// Accept connection on control socket.
	go func() {
		controlSocket, err := controlConn.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				execLog.WithFields(log.Fields{
					"err": err,
				}).Error("Error accepting control socket connection")
				return
			}
		}
		defer controlSocket.Close()
	}()

	// Generate input for DoTS app environment. Per https://pkg.go.dev/os/exec,
	// the i'th entry of cmd.ExtraFiles is mapped to FD 3 + i, so the loop
	// relies only on the lengths inputFiles, outputFiles, and sockets.
	dotsEnvInput := ""
	dotsEnvInput += strconv.Itoa(ourRank) + "\n"
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
	dotsEnvInput += app.GetFuncName() + "\n"

	// Write environment to stdin.
	if err != nil {
		execLog.WithFields(log.Fields{
			"err": err,
		}).Error("Error opening application stdin pipe")
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}
	if _, err := stdin.Write([]byte(dotsEnvInput)); err != nil {
		execLog.WithFields(log.Fields{
			"err": err,
		}).Error("Error writing environment to application stdin")
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	// Wait for application to finish.
	if err := cmd.Wait(); err != nil {
		execLog.WithFields(log.Fields{
			"err": err,
		}).Warn("Application exited with non-zero return code")
		return nil, grpc.Errorf(codes.Internal, "Application error")
	}

	return &dotspb.Result{Result: "success"}, nil
}
