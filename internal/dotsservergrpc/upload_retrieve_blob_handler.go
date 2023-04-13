package dotsservergrpc

import (
	"context"
	"os"
	"path"

	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dtrust-server/internal/util"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
)

func (s *DotsServerGrpc) UploadBlob(ctx context.Context, blob *dotspb.Blob) (*dotspb.Result, error) {
	// Make directory for the blob.
	blobDir := path.Join(s.config.FileStorageDir, s.config.OurNodeId, blob.GetClientId())
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		util.LoggerFromContext(ctx).Error("Error creating blob directory",
			"err", err,
			"blobDir", blobDir,
		)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	// Write blob data to file.
	blobPath := path.Join(blobDir, blob.GetKey())
	if err := os.WriteFile(blobPath, blob.GetVal(), 0644); err != nil {
		util.LoggerFromContext(ctx).Error("Error writing blob contents", "err", err,
			"blobPath", blobPath,
		)
		return nil, grpc.Errorf(codes.Internal, internalErrMsg)
	}

	return &dotspb.Result{Result: "success"}, nil
}

func (s *DotsServerGrpc) RetrieveBlob(ctx context.Context, blob *dotspb.Blob) (*dotspb.Blob, error) {
	// Attempt to read blob data.
	blobPath := path.Join(s.config.FileStorageDir, s.config.OurNodeId, blob.GetClientId(), blob.GetKey())
	blobData, err := os.ReadFile(blobPath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("Error reading blob contents",
				"err", err,
				"blobPath", blobPath,
			)
			return nil, grpc.Errorf(codes.Internal, internalErrMsg)
		}

		// Handle not found error.
		return nil, grpc.Errorf(codes.NotFound, "Blob with key not found")
	}

	blob.Val = blobData

	return blob, nil
}
