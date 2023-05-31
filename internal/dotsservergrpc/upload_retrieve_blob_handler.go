// Copyright 2023 The Dots Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dotsservergrpc

import (
	"context"
	"os"
	"path"

	"github.com/dtrust-project/dotspb/go/dotspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dtrust-project/dots-server/internal/util"
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
			util.LoggerFromContext(ctx).Error("Error reading blob contents",
				"err", err,
				"blobPath", blobPath,
			)
			return nil, grpc.Errorf(codes.Internal, internalErrMsg)
		}

		// Handle not found error.
		return nil, grpc.Errorf(codes.NotFound, "Blob with key not found: %s", blob.GetKey())
	}

	blob.Val = blobData

	return blob, nil
}
