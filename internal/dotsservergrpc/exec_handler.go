package dotsservergrpc

import (
	"context"
	"os"
	"path"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dtrust-server/internal/appinstance"
	"github.com/dtrust-project/dtrust-server/internal/serverconn"
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

	appLog.Debug("Executing appliation")

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

	// Register connection channels.
	// TODO Use an actual ID rather than uuid.Nil to disambiguate registrations.
	conns, err := s.conns.Register(ctx, serverconn.MsgTypeAppInstance, uuid.Nil)
	if err != nil {
		appLog.WithError(err).Error("Error registering app instance server connection")
		return nil, err
	}
	defer s.conns.Unregister(serverconn.MsgTypeAppInstance, uuid.Nil)

	// Start app.
	instance, err := appinstance.ExecApp(ctx, s.config, appConfig.Path, app.GetAppName(), app.GetFuncName(), inputFiles, outputFiles, conns)
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
