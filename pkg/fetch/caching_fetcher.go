package fetch

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbarn/bb-remote-asset/pkg/proto/asset"
	"github.com/buildbarn/bb-remote-asset/pkg/qualifier"
	"github.com/buildbarn/bb-remote-asset/pkg/storage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/protobuf/types/known/timestamppb"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingFetcher struct {
	fetcher    Fetcher
	assetStore storage.AssetStore
}

// NewCachingFetcher creates a decorator for remoteasset.FetchServer implementations to avoid having to fetch the
// blob remotely multiple times
func NewCachingFetcher(fetcher Fetcher, assetStore storage.AssetStore) Fetcher {
	return &cachingFetcher{
		fetcher:    fetcher,
		assetStore: assetStore,
	}
}

func (cf *cachingFetcher) FetchBlob(ctx context.Context, req *remoteasset.FetchBlobRequest) (*remoteasset.FetchBlobResponse, error) {
	instanceName, err := digest.NewInstanceName(req.InstanceName)

	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", req.InstanceName)
	}

	assetRef, digest, err := (&storage.GetRequest{
		InstanceName:          instanceName,
		OldestContentAccepted: req.OldestContentAccepted,
		Uris:                  req.Uris,
		Qualifiers:            req.Qualifiers,
	}).DoGet(ctx, cf.assetStore)

	if err != nil {
		return nil, err
	}

	if digest != nil {
		if len(assetRef.Uris) != 1 {
			panic("storage.Get should create assetRef with one URI")
		}

		// Successful retrieval from the asset reference cache
		return &remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "Blob fetched successfully from asset cache").Proto(),
			Uri:        assetRef.Uris[0],
			Qualifiers: req.Qualifiers,
			BlobDigest: digest,
		}, nil
	}

	// Cache Miss
	// Fetch from wrapped fetcher
	response, err := cf.fetcher.FetchBlob(ctx, req)
	if err != nil {
		return nil, err
	}
	if response.Status.Code != 0 {
		return response, nil
	}

	// Cache fetched blob with the single URI from Response.
	err = (&storage.PutRequest{
		InstanceName: instanceName,
		Uris:         []string{response.Uri},
		Qualifiers:   response.Qualifiers,
		ExpireAt:     getDefaultTimestamp(),
		Digest:       response.BlobDigest,
		AssetType:    asset.Asset_BLOB,
	}).DoPut(ctx, cf.assetStore)

	if err != nil {
		return response, fmt.Errorf("PushBlob failed putting asset: %w", err)
	}

	// Cache fetched blob using uris/qualifiers from Request.
	err = (&storage.PutRequest{
		InstanceName: instanceName,
		Uris:         req.Uris,
		Qualifiers:   req.Qualifiers,
		ExpireAt:     getDefaultTimestamp(),
		Digest:       response.BlobDigest,
		AssetType:    asset.Asset_BLOB,
	}).DoPut(ctx, cf.assetStore)

	if err != nil {
		return nil, fmt.Errorf("PushBlob failed putting asset from request: %w", err)
	}

	return response, nil
}

func (cf *cachingFetcher) FetchDirectory(ctx context.Context, req *remoteasset.FetchDirectoryRequest) (*remoteasset.FetchDirectoryResponse, error) {

	instanceName, err := digest.NewInstanceName(req.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", req.InstanceName)
	}

	assetRef, digest, err := (&storage.GetRequest{
		InstanceName:          instanceName,
		OldestContentAccepted: req.OldestContentAccepted,
		Uris:                  req.Uris,
		Qualifiers:            req.Qualifiers,
	}).DoGet(ctx, cf.assetStore)

	if err != nil {
		return nil, err
	}

	if digest != nil {
		if len(assetRef.Uris) != 1 {
			panic("storage.Get should create assetRef with one URI")
		}

		// Successful retrieval from the asset reference cache
		return &remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "Directory fetched successfully from asset cache").Proto(),
			Uri:                 assetRef.Uris[0],
			Qualifiers:          req.Qualifiers,
			RootDirectoryDigest: digest,
		}, nil
	}

	// Cache Miss
	// Fetch from wrapped fetcher
	response, err := cf.fetcher.FetchDirectory(ctx, req)
	if err != nil {
		return nil, err
	}

	// Cache fetched blob with the single URI from Response.
	err = (&storage.PutRequest{
		InstanceName: instanceName,
		Uris:         []string{response.Uri},
		Qualifiers:   response.Qualifiers,
		ExpireAt:     getDefaultTimestamp(),
		Digest:       response.RootDirectoryDigest,
		AssetType:    asset.Asset_DIRECTORY,
	}).DoPut(ctx, cf.assetStore)

	if err != nil {
		return response, fmt.Errorf("PushDirectory failed putting asset: %w", err)
	}

	// Cache fetched blob using uris/qualifiers from Request.
	err = (&storage.PutRequest{
		InstanceName: instanceName,
		Uris:         req.Uris,
		Qualifiers:   req.Qualifiers,
		ExpireAt:     getDefaultTimestamp(),
		Digest:       response.RootDirectoryDigest,
		AssetType:    asset.Asset_DIRECTORY,
	}).DoPut(ctx, cf.assetStore)

	if err != nil {
		return nil, fmt.Errorf("PushDirectory failed putting asset (from request): %w", err)
	}

	return response, nil
}

func (cf *cachingFetcher) CheckQualifiers(qualifiers qualifier.Set) qualifier.Set {
	return cf.fetcher.CheckQualifiers(qualifiers)
}

func getDefaultTimestamp() *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, 0))
}
