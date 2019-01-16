package goisilon

import (
	"context"
	"fmt"
	"path"

	api "github.com/tenortim/goisilon/api/v1"
)

// SnapshotList represents a list of Isilon snapshots.
type SnapshotList []*api.IsiSnapshot

// Snapshot represents an Isilon snapshot.
type Snapshot *api.IsiSnapshot

// GetSnapshots returns a list of snapshots from the cluster.
func (c *Client) GetSnapshots(ctx context.Context) (SnapshotList, error) {
	snapshots, err := api.GetIsiSnapshots(ctx, c.API)
	if err != nil {
		return nil, err
	}

	return snapshots.SnapshotList, nil
}

// GetSnapshotsByPath returns a list of snapshots covering the supplied path.
func (c *Client) GetSnapshotsByPath(
	ctx context.Context, path string) (SnapshotList, error) {

	snapshots, err := api.GetIsiSnapshots(ctx, c.API)
	if err != nil {
		return nil, err
	}
	// find all the snapshots with the same path
	snapshotsWithPath := make(SnapshotList, 0, len(snapshots.SnapshotList))
	for _, snapshot := range snapshots.SnapshotList {
		if snapshot.Path == c.API.VolumePath(path) {
			snapshotsWithPath = append(snapshotsWithPath, snapshot)
		}
	}
	return snapshotsWithPath, nil
}

// GetSnapshot returns a snapshot matching id, or if that is not found, matching name
func (c *Client) GetSnapshot(
	ctx context.Context, id int64, name string) (Snapshot, error) {

	// if we have an id, use it to find the snapshot
	snapshot, err := api.GetIsiSnapshot(ctx, c.API, id)
	if err == nil {
		return snapshot, nil
	}

	// there's no id or it didn't match, iterate through all snapshots and match
	// based on name
	if name == "" {
		return nil, err
	}
	snapshotList, err := c.GetSnapshots(ctx)
	if err != nil {
		return nil, err
	}

	for _, snapshot = range snapshotList {
		if snapshot.Name == name {
			return snapshot, nil
		}
	}

	return nil, nil
}

// CreateSnapshot creates a snapshot called name of the given path.
func (c *Client) CreateSnapshot(
	ctx context.Context, path, name string) (Snapshot, error) {

	return api.CreateIsiSnapshot(ctx, c.API, c.API.VolumePath(path), name)
}

// RemoveSnapshot removes the snapshot by id, or failing that, the snapshot matching name.
func (c *Client) RemoveSnapshot(
	ctx context.Context, id int64, name string) error {

	snapshot, err := c.GetSnapshot(ctx, id, name)
	if err != nil {
		return err
	}

	return api.RemoveIsiSnapshot(ctx, c.API, snapshot.Id)
}

// CopySnapshot copies all files/directories in a snapshot to a new directory.
func (c *Client) CopySnapshot(
	ctx context.Context,
	sourceID int64, sourceName, destinationName string) (Volume, error) {

	snapshot, err := c.GetSnapshot(ctx, sourceID, sourceName)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("Snapshot doesn't exist: (%d, %s)", sourceID, sourceName)
	}

	_, err = api.CopyIsiSnapshot(
		ctx, c.API, snapshot.Name,
		path.Base(snapshot.Path), destinationName)
	if err != nil {
		return nil, err
	}

	return c.GetVolume(ctx, destinationName, destinationName)
}
