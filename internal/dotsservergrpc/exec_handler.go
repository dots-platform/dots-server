package dotsservergrpc

import (
	"context"
	"encoding/binary"

	"github.com/dtrust-project/dotspb/go/dotspb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dots-server/internal/appinstance"
	"github.com/dtrust-project/dots-server/internal/util"
)

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

	ctx = util.ContextWithLogger(ctx, util.LoggerFromContext(ctx).With(
		"requestId", requestId,
	))

	// Look up app instance.
	var appInstance *appinstance.AppInstance
	if err := func() error {
		s.appsMutex.RLock()
		defer s.appsMutex.RUnlock()
		instance, ok := s.apps[app.GetAppName()]
		if !ok {
			util.LoggerFromContext(ctx).Error("App config present but app not found")
			return grpc.Errorf(codes.NotFound, "App with name not found")
		}
		appInstance = instance
		return nil
	}(); err != nil {
		return nil, err
	}

	util.LoggerFromContext(ctx).Debug("Executing appliation request")

	// Start app.
	request, err := appInstance.NewRequest(ctx, requestId, app.GetFuncName(), app.GetArgs())
	if err != nil {
		util.LoggerFromContext(ctx).Error("Failed to construct new app request", "err", err)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}
	output, err := request.Wait()
	if err != nil {
		util.LoggerFromContext(ctx).Error("Error spawning app instance", "err", err)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	util.LoggerFromContext(ctx).Info("Application finished")
	util.LoggerFromContext(ctx).Debug("Application finished with output",
		"output", output,
	)

	return &dotspb.Result{
		Result: "success",
		Output: output,
	}, nil
}
