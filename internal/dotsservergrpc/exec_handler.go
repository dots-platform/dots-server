package dotsservergrpc

import (
	"context"
	"encoding/binary"
	"encoding/gob"
	"os"
	"path"

	"github.com/dtrust-project/dotspb/go/dotspb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dots-server/internal/appinstance"
	"github.com/dtrust-project/dots-server/internal/serverconn"
	"github.com/dtrust-project/dots-server/internal/util"
)

type execBarrierTag struct {
	InstanceId uuid.UUID
}

func init() {
	gob.Register(execBarrierTag{})
}

func (s *DotsServerGrpc) Exec(ctx context.Context, app *dotspb.App) (*dotspb.Result, error) {
	ctx = util.ContextWithLogger(ctx, util.LoggerFromContext(ctx).With(
		"appName", app.GetAppName(),
		"appFuncName", app.GetFuncName(),
	))

	var requestIdBytes []byte
	requestIdBytes = binary.BigEndian.AppendUint64(requestIdBytes, app.GetRequestId().GetHi())
	requestIdBytes = binary.BigEndian.AppendUint64(requestIdBytes, app.GetRequestId().GetLo())
	requestId, err := uuid.FromBytes(requestIdBytes)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to parse request UUID from request", "err", err)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}
	if requestId == uuid.Nil {
		util.LoggerFromContext(ctx).Warn("Nil request UUID in request")
		return nil, grpc.Errorf(codes.InvalidArgument, "Request ID invalid")
	}

	// Look up app config.
	appConfig, ok := s.config.Apps[app.GetAppName()]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "App with name not found")
	}

	ctx = util.ContextWithLogger(ctx, util.LoggerFromContext(ctx).With(
		"requestId", requestId,
	))

	util.LoggerFromContext(ctx).Debug("Executing appliation")

	// Open input files.
	inputFiles := make([]*os.File, len(app.GetInFiles()))
	for i, inputName := range app.GetInFiles() {
		inputPath := path.Join(s.config.FileStorageDir, s.config.OurNodeId, app.GetClientId(), inputName)
		inputFile, err := os.Open(inputPath)
		if err != nil {
			if !os.IsNotExist(err) {
				util.LoggerFromContext(ctx).Error("Error opening input file",
					"err", err,
					"blobPath", inputPath,
				)
				return nil, err
			}

			// Handle not found error.
			return nil, grpc.Errorf(codes.NotFound, "Blob with key not found: %s", inputName)
		}
		defer inputFile.Close()
		inputFiles[i] = inputFile
	}

	// Open output files.
	outputFiles := make([]*os.File, len(app.GetOutFiles()))
	for i, outputName := range app.GetOutFiles() {
		outputDir := path.Join(s.config.FileStorageDir, s.config.OurNodeId, app.GetClientId())
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			util.LoggerFromContext(ctx).Error("Failed to create output directory",
				"err", err,
				"blobDir", outputDir,
			)
			return nil, grpc.Errorf(codes.Internal, internalErrMsg)
		}
		outputPath := path.Join(s.config.FileStorageDir, s.config.OurNodeId, app.GetClientId(), outputName)
		outputFile, err := os.Create(outputPath)
		if err != nil {
			util.LoggerFromContext(ctx).Error("Error opening output file",
				"err", err,
				"blobPath", outputPath,
			)
			return nil, grpc.Errorf(codes.Internal, internalErrMsg)
		}
		defer outputFile.Close()
		outputFiles[i] = outputFile
	}

	// Register connection channels.
	conns, err := s.conns.Register(ctx, serverconn.MsgTypeAppInstance, requestId)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error registering app instance server connection", "err", err)
		return nil, err
	}
	defer s.conns.Unregister(serverconn.MsgTypeAppInstance, requestId)

	// Barrier to ensure all other nodes have registered their communication
	// channels.
	s.controlComm.Barrier(execBarrierTag{requestId})

	// Start app.
	instance, err := appinstance.ExecApp(ctx, s.config, appConfig.Path, app.GetAppName(), app.GetFuncName(), inputFiles, outputFiles, app.GetArgs(), conns)
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error spawning app instance", "err", err)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}
	output, err := instance.Wait()
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error spawning app instance", "err", err)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	util.LoggerFromContext(ctx).Info("Application finished",
		"output", output,
	)
	util.LoggerFromContext(ctx).Debug("Application finished with output",
		"output", output,
	)

	return &dotspb.Result{
		Result: "success",
		Output: output,
	}, nil
}
