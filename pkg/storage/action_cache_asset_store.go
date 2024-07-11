package storage

import (
	"context"
	"fmt"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-asset/pkg/proto/asset"
	"github.com/buildbarn/bb-remote-asset/pkg/qualifier"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type actionCacheAssetStore struct {
	actionCache               blobstore.BlobAccess
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int
}

// NewActionCacheAssetStore creates a new AssetStore which stores it's
// references as ActionResults in the Action Cache.
func NewActionCacheAssetStore(actionCache, contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int) AssetStore {
	return &actionCacheAssetStore{
		actionCache:               actionCache,
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (rs *actionCacheAssetStore) actionResultToAsset(ctx context.Context, a *remoteexecution.ActionResult, instance digest.InstanceName) (*asset.Asset, error) {
	digest := &remoteexecution.Digest{}
	assetType := asset.Asset_DIRECTORY

	// Check if there is an output directory in the action result
	for _, dir := range a.OutputDirectories {
		if dir.Path == "out" {
			digest = dir.RootDirectoryDigest
		}
	}

	if digest == nil || digest.Hash == "" {
		assetType = asset.Asset_BLOB
		// Required output directory is not present, look for required
		// output file
		for _, file := range a.OutputFiles {
			if file.Path == "out" {
				digest = file.Digest
			}
		}
	}

	if digest == nil || digest.Hash == "" {
		return nil, fmt.Errorf("could not find digest (either directory or blob) in ActionResult")
	}

	return &asset.Asset{
		Digest:      digest,
		ExpireAt:    getDefaultTimestamp(),
		LastUpdated: a.ExecutionMetadata.QueuedTimestamp,
		Type:        assetType,
	}, nil
}

func (rs *actionCacheAssetStore) Get(ctx context.Context, ref *asset.AssetReference, instance digest.InstanceName) (*asset.Asset, error) {
	// Create asset reference using only the qualifiers of the request
	qualifierReference := NewAssetReference(nil, ref.Qualifiers)
	refDigest, err := ProtoToDigest(qualifierReference)
	if err != nil {
		return nil, err
	}
	// Construct a directory using the reference of only qualifiers
	directory := &remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{{
			Name:   "AssetReference",
			Digest: refDigest,
		}},
	}
	directoryDigest, err := ProtoToDigest(directory)
	if err != nil {
		return nil, err
	}
	var action *remoteexecution.Action
	if commandGenerator, err := qualifier.QualifiersToCommand(ref.Qualifiers); err != nil || len(ref.Uris) > 1 {
		// Create the action with the qualifier directory as the input root
		action, _, err = assetReferenceToAction(ref, directoryDigest)
		if err != nil {
			return nil, err
		}
	} else {
		command := commandGenerator(ref.Uris[0])
		commandDigest, err := ProtoToDigest(command)
		if err != nil {
			return nil, err
		}
		action = &remoteexecution.Action{
			CommandDigest:   commandDigest,
			InputRootDigest: EmptyDigest,
		}
	}
	actionDigest, err := ProtoToDigest(action)
	if err != nil {
		return nil, err
	}
	digestFunction, err := instance.GetDigestFunction(remoteexecution.DigestFunction_UNKNOWN, len(actionDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	digest, err := digestFunction.NewDigestFromProto(actionDigest)
	if err != nil {
		return nil, err
	}

	data, err := rs.actionCache.Get(ctx, digest).ToProto(
		&remoteexecution.ActionResult{},
		rs.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return rs.actionResultToAsset(ctx, data.(*remoteexecution.ActionResult), instance)
}

func (rs *actionCacheAssetStore) Put(ctx context.Context, ref *asset.AssetReference, data *asset.Asset, instance digest.InstanceName) error {
	digestFunction, err := instance.GetDigestFunction(remoteexecution.DigestFunction_UNKNOWN, len(data.GetDigest().GetHash()))
	if err != nil {
		return err
	}
	// Create asset reference using only the qualifiers of the request
	qualifierReference := NewAssetReference(nil, ref.Qualifiers)
	refDigest, err := ProtoToDigest(qualifierReference)
	if err != nil {
		return err
	}
	refPb, err := proto.Marshal(qualifierReference)
	if err != nil {
		return err
	}
	bbRefDigest, err := AssetReferenceToDigest(qualifierReference, instance)
	if err != nil {
		return err
	}
	// Put the qualifier reference in the CAS to ensure completeness of
	// the action result
	err = rs.contentAddressableStorage.Put(ctx, bbRefDigest, buffer.NewCASBufferFromByteSlice(bbRefDigest, refPb, buffer.UserProvided))
	if err != nil {
		return err
	}
	// Construct a directory using the reference of only qualifiers
	// This is how qualifiers are linked to the assets when stored as
	// action results
	directory := &remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{{
			Name:   "AssetReference",
			Digest: refDigest,
		}},
	}
	directoryPb, err := proto.Marshal(directory)
	if err != nil {
		return err
	}
	directoryDigest, err := ProtoToDigest(directory)
	if err != nil {
		return err
	}
	bbDirectoryDigest, err := digestFunction.NewDigestFromProto(directoryDigest)
	if err != nil {
		return nil
	}
	err = rs.contentAddressableStorage.Put(ctx, bbDirectoryDigest, buffer.NewCASBufferFromByteSlice(bbDirectoryDigest, directoryPb, buffer.UserProvided))
	if err != nil {
		return err
	}
	var action *remoteexecution.Action
	var command *remoteexecution.Command
	if commandGenerator, err := qualifier.QualifiersToCommand(ref.Qualifiers); err != nil || len(ref.Uris) > 1 {
		// Create the action with the qualifier directory as the input root
		action, command, err = assetReferenceToAction(ref, directoryDigest)
		if err != nil {
			return err
		}
	} else {
		command = commandGenerator(ref.Uris[0])
		commandDigest, err := ProtoToDigest(command)
		if err != nil {
			return err
		}
		action = &remoteexecution.Action{
			CommandDigest:   commandDigest,
			InputRootDigest: EmptyDigest,
		}
	}
	actionPb, err := proto.Marshal(action)
	if err != nil {
		return err
	}
	actionDigest, err := ProtoToDigest(action)
	if err != nil {
		return err
	}
	bbActionDigest, err := digestFunction.NewDigestFromProto(actionDigest)
	if err != nil {
		return err
	}
	err = rs.contentAddressableStorage.Put(ctx, bbActionDigest, buffer.NewCASBufferFromByteSlice(bbActionDigest, actionPb, buffer.UserProvided))
	if err != nil {
		return err
	}

	commandPb, err := proto.Marshal(command)
	if err != nil {
		return err
	}
	commandDigest, err := ProtoToDigest(command)
	if err != nil {
		return err
	}
	bbCommandDigest, err := digestFunction.NewDigestFromProto(commandDigest)
	if err != nil {
		return err
	}
	err = rs.contentAddressableStorage.Put(ctx, bbCommandDigest, buffer.NewCASBufferFromByteSlice(bbCommandDigest, commandPb, buffer.UserProvided))
	if err != nil {
		return err
	}

	actionResult := &remoteexecution.ActionResult{
		ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
			QueuedTimestamp: data.LastUpdated,
		},
	}

	if data.Type == asset.Asset_DIRECTORY {
		// Use digest as a root directory digest
		actionResult.OutputDirectories = []*remoteexecution.OutputDirectory{{
			Path:                "out",
			RootDirectoryDigest: data.Digest,
		}}
	} else if data.Type == asset.Asset_BLOB {
		// Use the digest as an output file digest
		actionResult.OutputFiles = []*remoteexecution.OutputFile{{
			Path:   "out",
			Digest: data.Digest,
		}}
	} else {
		return fmt.Errorf("unknown asset type %v", data.Type)
	}

	return rs.actionCache.Put(ctx, bbActionDigest, buffer.NewProtoBufferFromProto(actionResult, buffer.UserProvided))
}

func getDefaultTimestamp() *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, 0))
}
