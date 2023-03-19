package dotsservergrpc

import (
	"context"
	"os"
	"path"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
)

const internalErrMsg = "Internal error"

type DotsServerGrpc struct {
	nodeId string
	config *config.Config

	dotspb.UnimplementedDecExecServer
}

// Assert DotsServerGrpc fulfills dotspb.DecExecServer.
var _ dotspb.DecExecServer = (*DotsServerGrpc)(nil)

func NewDotsServerGrpc(nodeId string, config *config.Config) (*DotsServerGrpc, error) {
	server := &DotsServerGrpc{
		nodeId: nodeId,
		config: config,
	}
	return server, nil
}

func (s *DotsServerGrpc) UploadBlob(ctx context.Context, blob *dotspb.Blob) (*dotspb.Result, error) {
	// Make directory for the blob.
	blobDir := path.Join(s.config.FileStorageDir, s.nodeId, blob.GetClientId())
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		log.WithFields(log.Fields{
			"blobDir": blobDir,
			"err":     err,
		}).Error("Error creating blob directory")
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	// Write blob data to file.
	blobPath := path.Join(blobDir, blob.GetKey())
	if err := os.WriteFile(blobPath, blob.GetVal(), 0644); err != nil {
		log.WithFields(log.Fields{
			"blobPath": blobPath,
			"err":      err,
		}).Error("Error writing blob contents")
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	return &dotspb.Result{Result: "success"}, nil
}
