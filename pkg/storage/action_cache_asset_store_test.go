package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-asset/internal/mock"
	"github.com/buildbarn/bb-remote-asset/pkg/proto/asset"
	"github.com/buildbarn/bb-remote-asset/pkg/storage"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestActionCacheAssetStorePutBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName := digest.MustNewInstanceName("")

	blobDigest := &remoteexecution.Digest{
		Hash:      "58de0f27ce0f781e5c109f18b0ee6905bdf64f2b1009e225ac67a27f656a0643",
		SizeBytes: 111,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{{Name: "test", Value: "test"}})
	assetData := storage.NewBlobAsset(blobDigest, timestamppb.Now())
	refDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"a2c2b32a289d4d9bf6e6309ed2691b6bcc04ee7923fcfd81bf1bfe0e7348139b",
		14,
	)
	directoryDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"c72e5e1e6ab54746d4fd3da7b443037187c81347a210d2ab8e5863638fbe1ac6",
		88,
	)
	actionDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"b1b0b54caccd4235e968061f380649a9720ee67909fd1027200230eba93a2427",
		140,
	)
	commandDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"7b9d720c2fbc4e4c0fc9780208eae45f4d2c4dc23350c53dc1050259827ca459",
		40,
	)

	ac := mock.NewMockBlobAccess(ctrl)
	cas := mock.NewMockBlobAccess(ctrl)
	cas.EXPECT().Put(ctx, refDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, directoryDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, actionDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, commandDigest, gomock.Any()).Return(nil)
	ac.EXPECT().Put(ctx, actionDigest, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			m, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
			require.NoError(t, err)
			a := m.(*remoteexecution.ActionResult)
			for _, f := range a.OutputFiles {
				if f.Path == "out" {
					require.True(t, proto.Equal(f.Digest, blobDigest))
					return nil
				}
			}
			return status.Error(codes.Internal, "Blob digest not found")
		})
	assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

	err := assetStore.Put(ctx, assetRef, assetData, instanceName)
	require.NoError(t, err)
}

func TestActionCacheAssetStorePutDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := digest.NewInstanceName("")
	require.NoError(t, err)

	rootDirectoryDigest := &remoteexecution.Digest{
		Hash:      "58de0f27ce0f781e5c109f18b0ee6905bdf64f2b1009e225ac67a27f656a0643",
		SizeBytes: 111,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{{Name: "test", Value: "test"}})
	assetData := storage.NewAsset(
		rootDirectoryDigest,
		asset.Asset_DIRECTORY,
		timestamppb.Now(),
	)
	refDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"a2c2b32a289d4d9bf6e6309ed2691b6bcc04ee7923fcfd81bf1bfe0e7348139b",
		14,
	)
	directoryDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"c72e5e1e6ab54746d4fd3da7b443037187c81347a210d2ab8e5863638fbe1ac6",
		88,
	)
	actionDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"b1b0b54caccd4235e968061f380649a9720ee67909fd1027200230eba93a2427",
		140,
	)
	commandDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"7b9d720c2fbc4e4c0fc9780208eae45f4d2c4dc23350c53dc1050259827ca459",
		40,
	)

	ac := mock.NewMockBlobAccess(ctrl)
	cas := mock.NewMockBlobAccess(ctrl)
	cas.EXPECT().Put(ctx, refDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, directoryDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, actionDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, commandDigest, gomock.Any()).Return(nil)
	ac.EXPECT().Put(ctx, actionDigest, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			m, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
			require.NoError(t, err)
			a := m.(*remoteexecution.ActionResult)
			for _, d := range a.OutputDirectories {
				if d.Path == "out" {
					require.True(t, proto.Equal(d.RootDirectoryDigest, rootDirectoryDigest))
					return nil
				}
			}
			return status.Error(codes.Internal, "Directory digest not found")
		})
	assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

	err = assetStore.Put(ctx, assetRef, assetData, instanceName)
	require.NoError(t, err)
}

func TestActionCacheAssetStorePutMalformedDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := digest.NewInstanceName("")
	require.NoError(t, err)

	rootDirectoryDigest := &remoteexecution.Digest{
		Hash:      "58de0f27ce0f781e5c109f18b0ee6905bdf64f2b1009e225ac67a27f656a0643",
		SizeBytes: 111,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{{Name: "test", Value: "test"}})
	assetData := storage.NewAsset(
		rootDirectoryDigest,
		asset.Asset_DIRECTORY,
		timestamppb.Now(),
	)
	refDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"a2c2b32a289d4d9bf6e6309ed2691b6bcc04ee7923fcfd81bf1bfe0e7348139b",
		14,
	)
	directoryDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"c72e5e1e6ab54746d4fd3da7b443037187c81347a210d2ab8e5863638fbe1ac6",
		88,
	)
	actionDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"b1b0b54caccd4235e968061f380649a9720ee67909fd1027200230eba93a2427",
		140,
	)
	commandDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"7b9d720c2fbc4e4c0fc9780208eae45f4d2c4dc23350c53dc1050259827ca459",
		40,
	)

	ac := mock.NewMockBlobAccess(ctrl)
	cas := mock.NewMockBlobAccess(ctrl)
	cas.EXPECT().Put(ctx, refDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, directoryDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, actionDigest, gomock.Any()).Return(nil)
	cas.EXPECT().Put(ctx, commandDigest, gomock.Any()).Return(nil)
	ac.EXPECT().Put(ctx, actionDigest, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			m, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
			require.NoError(t, err)
			a := m.(*remoteexecution.ActionResult)
			for _, d := range a.OutputDirectories {
				if d.Path == "out" {
					require.True(t, proto.Equal(d.RootDirectoryDigest, rootDirectoryDigest))
					return nil
				}
			}
			return status.Error(codes.Internal, "Directory digest not found")
		})
	assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

	err = assetStore.Put(ctx, assetRef, assetData, instanceName)
	require.NoError(t, err)
}

func roundTripTest(t *testing.T, assetRef *asset.AssetReference, assetData *asset.Asset) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := digest.NewInstanceName("")
	require.NoError(t, err)

	var actionDigest digest.Digest
	var actionResult *remoteexecution.ActionResult

	{
		ac := mock.NewMockBlobAccess(ctrl)
		cas := mock.NewMockBlobAccess(ctrl)

		cas.EXPECT().Put(ctx, gomock.Any(), gomock.Any()).AnyTimes()
		ac.EXPECT().Put(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				actionDigest = digest
				m, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
				require.NoError(t, err)
				actionResult = m.(*remoteexecution.ActionResult)
				return nil
			})

		assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

		err = assetStore.Put(ctx, assetRef, assetData, instanceName)
		require.NoError(t, err)
	}
	{
		require.NotNil(t, actionResult)

		ac := mock.NewMockBlobAccess(ctrl)
		cas := mock.NewMockBlobAccess(ctrl)

		ac.EXPECT().Get(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest) buffer.Buffer {
				if digest == actionDigest {
					return buffer.NewProtoBufferFromProto(actionResult, buffer.UserProvided)
				}
				return buffer.NewBufferFromError(fmt.Errorf("not in AC"))
			})

		assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

		asset, err := assetStore.Get(ctx, assetRef, instanceName)
		require.NoError(t, err)
		require.Equal(t, asset.Digest, assetData.Digest)
	}
}

func TestActionCacheAssetStoreRoundTrip(t *testing.T) {
	expectedDigest := &remoteexecution.Digest{
		Hash:      "58de0f27ce00781e5c109f18b0ee6905bdf64f2b1009e225ac67a27f656a0643",
		SizeBytes: 115,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{{Name: "test", Value: "test"}})

	assetData := storage.NewBlobAsset(expectedDigest, timestamppb.Now())

	roundTripTest(t, assetRef, assetData)
}

func TestActionCacheAssetStoreRoundTripDirectory(t *testing.T) {
	expectedDigest := &remoteexecution.Digest{
		Hash:      "58de0f27ce00781e5c109f18b0ee6905bdf64f2b1009e225ac67a27f656a0643",
		SizeBytes: 115,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{{Name: "test", Value: "test"}})

	assetData := storage.NewAsset(
		expectedDigest,
		asset.Asset_DIRECTORY,
		timestamppb.Now(),
	)

	roundTripTest(t, assetRef, assetData)
}

func TestActionCacheAssetStoreRoundTripWithSpecialQualifiers(t *testing.T) {
	expectedDigest := &remoteexecution.Digest{
		Hash:      "58de0f27ce00781e5c109f18b0ee6905bdf64f2b1009e225ac67a27f656a0643",
		SizeBytes: 115,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{{Name: "resource_type", Value: "application/x-git"}})

	assetData := storage.NewBlobAsset(expectedDigest, timestamppb.Now())

	roundTripTest(t, assetRef, assetData)
}

func TestActionCacheAssetStoreGetBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName := digest.MustNewInstanceName("")

	blobDigest := &remoteexecution.Digest{
		Hash:      "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f",
		SizeBytes: 222,
	}
	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{})
	actionDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"b990c9040edaddd33b6e7506c23500cb4d9f699c780089aaa46c5d5f9479f74e",
		140,
	)

	ts := timestamppb.New(time.Unix(0, 0))
	buf := buffer.NewProtoBufferFromProto(&remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path:   "out",
				Digest: blobDigest,
			},
		},
		ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
			QueuedTimestamp: ts,
		},
	}, buffer.UserProvided)

	ac := mock.NewMockBlobAccess(ctrl)
	cas := mock.NewMockBlobAccess(ctrl)
	ac.EXPECT().Get(ctx, actionDigest).Return(buf)
	assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

	_, err := assetStore.Get(ctx, assetRef, instanceName)
	require.NoError(t, err)
}

func TestActionCacheAssetStoreGetDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName := digest.MustNewInstanceName("")

	uri := "https://example.com/example.txt"
	assetRef := storage.NewAssetReference([]string{uri},
		[]*remoteasset.Qualifier{})
	actionDigest := digest.MustNewDigest(
		"",
		remoteexecution.DigestFunction_SHA256,
		"b990c9040edaddd33b6e7506c23500cb4d9f699c780089aaa46c5d5f9479f74e",
		140,
	)

	ts := timestamppb.New(time.Unix(0, 0))
	buf := buffer.NewProtoBufferFromProto(&remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "out",
				Digest: &remoteexecution.Digest{
					Hash:      "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f",
					SizeBytes: 222,
				},
			},
		},
		ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
			QueuedTimestamp: ts,
		},
	}, buffer.UserProvided)

	ac := mock.NewMockBlobAccess(ctrl)
	cas := mock.NewMockBlobAccess(ctrl)
	ac.EXPECT().Get(ctx, actionDigest).Return(buf)
	assetStore := storage.NewActionCacheAssetStore(ac, cas, 16*1024*1024)

	_, err := assetStore.Get(ctx, assetRef, instanceName)
	require.NoError(t, err)
}
