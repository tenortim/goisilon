package v1

import (
	"context"
	"errors"
	"fmt"

	"github.com/tenortim/goisilon/api"
)

// GetIsiQuota queries the quota for a directory
func GetIsiQuota(
	ctx context.Context,
	client api.Client,
	path string) (quota *IsiQuota, err error) {

	// PAPI call: GET https://1.2.3.4:8080/platform/1/quota/quotas?path=path
	// This will list quotas matching path on the cluster

	var quotaResp isiQuotaListResp
	err = client.Get(ctx, quotaPath, "", api.OrderedValues{{byteArrPath, []byte(path)}}, nil, &quotaResp)
	if err != nil {
		return nil, err
	}

	// find the specific quota we are looking for
	for _, quota := range quotaResp.Quotas {
		if quota.Path == path {
			return &quota, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("Quota not found: %s", path))
}

// TODO: Add a means to set/update more than just the hard threshold

// CreateIsiQuota creates a hard directory quota on given path
func CreateIsiQuota(
	ctx context.Context,
	client api.Client,
	path string, container bool, size int64) (err error) {

	// PAPI call: POST https://1.2.3.4:8080/platform/1/quota/quotas
	//             { "enforced" : true,
	//               "include_snapshots" : false,
	//               "path" : "/ifs/volumes/volume_name",
	//               "container" : true,
	//               "thresholds_include_overhead" : false,
	//               "type" : "directory",
	//               "thresholds" : { "advisory" : null,
	//                                "hard" : 1234567890,
	//                                "soft" : null
	//                              }
	//             }
	var data = &IsiQuotaReq{
		Enforced:                  true,
		IncludeSnapshots:          false,
		Path:                      path,
		Container:                 container,
		ThresholdsIncludeOverhead: false,
		Type:                      "directory",
		Thresholds:                isiThresholdsReq{Advisory: nil, Hard: size, Soft: nil},
	}

	var quotaResp IsiQuota
	err = client.Post(ctx, quotaPath, "", nil, nil, data, &quotaResp)
	return err
}

// SetIsiQuotaHardThreshold sets the hard threshold of a quota for a directory
// This is really just CreateIsiQuota() with container set to false
func SetIsiQuotaHardThreshold(
	ctx context.Context,
	client api.Client,
	path string, size int64) (err error) {

	return CreateIsiQuota(ctx, client, path, false, size)
}

// UpdateIsiQuotaHardThreshold modifies the hard threshold of a quota for a directory
func UpdateIsiQuotaHardThreshold(
	ctx context.Context,
	client api.Client,
	path string, size int64) (err error) {

	// PAPI call: PUT https://1.2.3.4:8080/platform/1/quota/quotas/Id
	//             { "enforced" : true,
	//               "thresholds_include_overhead" : false,
	//               "thresholds" : { "advisory" : null,
	//                                "hard" : 1234567890,
	//                                "soft" : null
	//                              }
	//             }
	var data = &IsiUpdateQuotaReq{
		Enforced:                  true,
		ThresholdsIncludeOverhead: false,
		Thresholds:                isiThresholdsReq{Advisory: nil, Hard: size, Soft: nil},
	}

	quota, err := GetIsiQuota(ctx, client, path)
	if err != nil {
		return err
	}

	var quotaResp IsiQuota
	err = client.Put(ctx, quotaPath, quota.Id, nil, nil, data, &quotaResp)
	return err
}

var byteArrPath = []byte("path")

// DeleteIsiQuota removes the quota for a directory
func DeleteIsiQuota(
	ctx context.Context,
	client api.Client,
	path string) (err error) {

	// PAPI call: DELETE https://1.2.3.4:8080/platform/1/quota/quotas?path=/path/to/volume
	// This will remove a the quota on a volume

	return client.Delete(
		ctx,
		quotaPath,
		"",
		api.OrderedValues{{byteArrPath, []byte(path)}},
		nil,
		nil)
}
