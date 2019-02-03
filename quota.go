package goisilon

import (
	"context"

	api "github.com/tenortim/goisilon/api/v1"
)

// Quota maps to an Isilon filesystem quota.
type Quota *api.IsiQuota

// GetQuota returns a specific quota by path
func (c *Client) GetQuota(ctx context.Context, name string) (Quota, error) {
	quota, err := api.GetIsiQuota(ctx, c.API, c.API.VolumePath(name))
	if err != nil {
		return nil, err
	}

	return quota, nil
}

// TODO: Add a means to set/update more fields of the quota

// CreateQuota creates a new hard directory quota with the specified size
// and container option
func (c *Client) CreateQuota(
	ctx context.Context, name string, container bool, size int64) error {

	return api.CreateIsiQuota(
		ctx, c.API, c.API.VolumePath(name), container, size)
}


// SetQuotaSize sets the max size (hard threshold) of a quota for a volume
func (c *Client) SetQuotaSize(
	ctx context.Context, name string, size int64) error {

	return api.SetIsiQuotaHardThreshold(
		ctx, c.API, c.API.VolumePath(name), size)
}

// UpdateQuotaSize modifies the max size (hard threshold) of a quota for a volume
func (c *Client) UpdateQuotaSize(
	ctx context.Context, name string, size int64) error {

	return api.UpdateIsiQuotaHardThreshold(
		ctx, c.API, c.API.VolumePath(name), size)
}

// ClearQuota removes the quota from a volume
func (c *Client) ClearQuota(ctx context.Context, name string) error {
	return api.DeleteIsiQuota(ctx, c.API, c.API.VolumePath(name))
}
