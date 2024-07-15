package storage

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-asset/pkg/proto/asset"
	bb_digest "github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
)

// AssetStore is a wrapper around a BlobAccess to inteface well with
// AssetReference messages
type AssetStore interface {
	Get(ctx context.Context, req *asset.AssetReference, instance bb_digest.InstanceName) (*asset.Asset, error)
	Put(ctx context.Context, req *asset.AssetReference, data *asset.Asset, instance bb_digest.InstanceName) error
}

type GetRequest struct {
	InstanceName          bb_digest.InstanceName
	OldestContentAccepted *timestamppb.Timestamp
	Uris                  []string
	Qualifiers            []*remoteasset.Qualifier
}

func (req *GetRequest) DoGet(ctx context.Context, a AssetStore) (*asset.AssetReference, *remoteexecution.Digest, error) {
	var oldestContentAccepted time.Time = time.Unix(0, 0)
	if req.OldestContentAccepted != nil {
		if err := req.OldestContentAccepted.CheckValid(); err != nil {
			return nil, nil, err
		}
		oldestContentAccepted = req.OldestContentAccepted.AsTime()
	}

	// Check assetStore
	for _, uri := range req.Uris {
		assetRef := NewAssetReference([]string{uri}, req.Qualifiers)
		assetData, err := a.Get(ctx, assetRef, req.InstanceName)
		if err != nil {
			continue
		}

		// Check whether the asset has expired, making sure ExpireAt was set
		if assetData.ExpireAt != nil {
			expireTime := assetData.ExpireAt.AsTime()
			if expireTime.Before(time.Now()) && !expireTime.Equal(time.Unix(0, 0)) {
				continue
			}
		}

		// Check that content is newer than the oldest accepted by the request
		if oldestContentAccepted != time.Unix(0, 0) {
			updateTime := assetData.LastUpdated.AsTime()
			if updateTime.Before(oldestContentAccepted) {
				continue
			}
		}

		if len(assetRef.Uris) != 1 {
			panic("We created assetRef with one URI")
		}

		return assetRef, assetData.Digest, nil
	}

	return nil, nil, nil
}

type PutRequest struct {
	InstanceName bb_digest.InstanceName
	Uris                  []string
	Qualifiers            []*remoteasset.Qualifier
	ExpireAt     *timestamppb.Timestamp
	Digest       *remoteexecution.Digest
	AssetType    asset.Asset_AssetType
}

func (req *PutRequest) DoPut(ctx context.Context, a AssetStore) error {
	if len(req.Uris) == 0 {
		return status.Errorf(codes.InvalidArgument, "At least one URI required")
	}

	assetRef := NewAssetReference(req.Uris, req.Qualifiers)
	assetData := NewAsset(req.Digest, req.AssetType, req.ExpireAt)
	err := a.Put(ctx, assetRef, assetData, req.InstanceName)
	if err != nil {
		return err
	}

	if len(req.Uris) > 1 {
		for _, uri := range req.Uris {
			assetRef := NewAssetReference([]string{uri}, req.Qualifiers)
			assetData := NewAsset(req.Digest, req.AssetType, req.ExpireAt)
			err = a.Put(ctx, assetRef, assetData, req.InstanceName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
