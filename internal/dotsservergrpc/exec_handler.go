package dotsservergrpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"

	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dtrust-server/internal/appinstance"
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

	appLog := log.WithFields(log.Fields{
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
	ourRank := s.config.NodeRanks[s.config.OurNodeId]
	ourConfig := s.config.Nodes[s.config.OurNodeId]
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
			appLog.WithError(err).Error("Error opening application socket")
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}

	// Open input files.
	inputFiles := make([]*os.File, len(app.GetInFiles()))
	for i, inputName := range app.GetInFiles() {
		inputPath := path.Join(s.config.FileStorageDir, s.config.OurNodeId, app.GetClientId(), inputName)
		inputFile, err := os.Open(inputPath)
		if err != nil {
			if !os.IsNotExist(err) {
				appLog.WithFields(log.Fields{
					"blobPath": inputPath,
				}).WithError(err).Error("Error opening input file")
				return nil, err
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
		outputPath := path.Join(s.config.FileStorageDir, s.config.OurNodeId, app.GetClientId(), outputName)
		outputFile, err := os.Create(outputPath)
		if err != nil {
			if !os.IsNotExist(err) {
				appLog.WithFields(log.Fields{
					"blobPath": outputPath,
				}).WithError(err).Error("Error opening output file")
				return nil, grpc.Errorf(codes.Internal, internalErrMsg)
			}

			// Handle not found error.
			return nil, grpc.Errorf(codes.NotFound, "Blob with key not found")
		}
		defer outputFile.Close()
		outputFiles[i] = outputFile
	}

	// Start app.
	instance, err := appinstance.ExecApp(ctx, s.config, appConfig.Path, app.GetAppName(), app.GetFuncName(), inputFiles, outputFiles, sockets)
	if err != nil {
		appLog.WithError(err).Error("Error spawning app instance")
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}
	if err := instance.Wait(); err != nil {
		appLog.WithError(err).Error("Error spawning app instance")
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	return &dotspb.Result{Result: "success"}, nil
}
