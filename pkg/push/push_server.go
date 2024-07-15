package push

import (
	"context"
	"fmt"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"github.com/buildbarn/bb-remote-asset/pkg/proto/asset"
	"github.com/buildbarn/bb-remote-asset/pkg/storage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type assetPushServer struct {
	assetStore               storage.AssetStore
	allowUpdatesForInstances map[digest.InstanceName]bool
}

// NewAssetPushServer creates a gRPC service for serving the contents
// of a Remote Asset Push server.
func NewAssetPushServer(AssetStore storage.AssetStore, allowUpdatesForInstances map[digest.InstanceName]bool) remoteasset.PushServer {
	return &assetPushServer{
		assetStore:               AssetStore,
		allowUpdatesForInstances: allowUpdatesForInstances,
	}
}

func (s *assetPushServer) instanceName(instanceNameStr string) (digest.InstanceName, error) {
	instanceName, err := digest.NewInstanceName(instanceNameStr)
	if err != nil {
		return digest.InstanceName{}, util.StatusWrapf(err, "Invalid instance name %#v", instanceNameStr)
	}

	if !s.allowUpdatesForInstances[instanceName] {
		return digest.InstanceName{}, status.Errorf(
			codes.PermissionDenied,
			"This service does not accept updates for instance %#v",
			instanceNameStr,
		)
	}

	return instanceName, nil
}

func (s *assetPushServer) PushBlob(ctx context.Context, req *remoteasset.PushBlobRequest) (*remoteasset.PushBlobResponse, error) {

	instanceName, err := s.instanceName(req.InstanceName)

	if err != nil {
		return nil, fmt.Errorf("PushBlob failed validating instance name: %w", err)
	}

	err = (&storage.PutRequest{
		InstanceName: instanceName,
		Uris:         req.Uris,
		Qualifiers:   req.Qualifiers,
		ExpireAt:     req.ExpireAt,
		Digest:       req.BlobDigest,
		AssetType:    asset.Asset_BLOB,
	}).DoPut(
		ctx,
		s.assetStore,
	)

	if err != nil {
		return nil, fmt.Errorf("PushBlob failed putting asset with eeror: %w", err)
	}

	return &remoteasset.PushBlobResponse{}, nil
}

func (s *assetPushServer) PushDirectory(ctx context.Context, req *remoteasset.PushDirectoryRequest) (*remoteasset.PushDirectoryResponse, error) {

	instanceName, err := s.instanceName(req.InstanceName)

	if err != nil {
		return nil, fmt.Errorf("PushDirectory failed validating instance name: %w", err)
	}

	err = (&storage.PutRequest{
		InstanceName: instanceName,
		Uris:         req.Uris,
		Qualifiers:   req.Qualifiers,
		ExpireAt:     req.ExpireAt,
		Digest:       req.RootDirectoryDigest,
		AssetType:    asset.Asset_DIRECTORY,
	}).DoPut(
		ctx,
		s.assetStore,
	)

	if err != nil {
		return nil, fmt.Errorf("PushDirectory failed with eeror: %w", err)
	}

	return &remoteasset.PushDirectoryResponse{}, nil
}
