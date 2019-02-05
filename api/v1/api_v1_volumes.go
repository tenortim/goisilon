package v1

import (
	"context"
	"path"

	"github.com/tenortim/goisilon/api"
)

const defaultACL = "public_read_write"

var (
	aclQS           = api.OrderedValues{{[]byte("acl")}}
	metadataQS      = api.OrderedValues{{[]byte("metadata")}}
	recursiveTrueQS = api.OrderedValues{
		{[]byte("recursive"), []byte("true")},
	}
)

// GetIsiVolumes queries a list of all volumes on the cluster
func GetIsiVolumes(
	ctx context.Context,
	client api.Client) (resp *getIsiVolumesResp, err error) {

	// PAPI call: GET https://1.2.3.4:8080/namespace/path/to/volumes/
	err = client.Get(ctx, realNamespacePath(client), "", nil, nil, &resp)
	return resp, err
}

// CreateIsiVolume makes a new volume on the cluster
func CreateIsiVolume(
	ctx context.Context,
	client api.Client,
	name string) (resp *getIsiVolumesResp, err error) {

	return CreateIsiVolumeWithACL(ctx, client, name, defaultACL)
}

// CreateIsiVolumeWithACL makes a new volume on the cluster with the specified permissions
func CreateIsiVolumeWithACL(
	ctx context.Context,
	client api.Client,
	name, ACL string) (resp *getIsiVolumesResp, err error) {

	// PAPI calls: PUT https://1.2.3.4:8080/namespace/path/to/volumes/volume_name
	//             x-isi-ifs-target-type: container
	//             x-isi-ifs-access-control: ACL
	//
	//             PUT https://1.2.3.4:8080/namespace/path/to/volumes/volume_name?acl
	//             {authoritative: "acl",
	//              action: "update",
	//              owner: {name: "username", type: "user"},
	//              group: {name: "groupname", type: "group"}
	//             }

	createVolumeHeaders := map[string]string{
		"x-isi-ifs-target-type":    "container",
		"x-isi-ifs-access-control": ACL,
	}

	// create the volume
	err = client.Put(
		ctx,
		realNamespacePath(client),
		name,
		nil,
		createVolumeHeaders,
		nil,
		&resp)

	if err != nil {
		return resp, err
	}

	var data = &AclRequest{
		"acl",
		"update",
		&Ownership{client.User(), "user"},
		nil,
	}

	if group := client.Group(); group != "" {
		data.Group = &Ownership{group, "group"}
	}

	// set the ownership of the volume
	err = client.Put(
		ctx,
		realNamespacePath(client),
		name,
		aclQS,
		nil,
		data,
		&resp)

	return resp, err
}

// GetIsiVolume queries the attributes of a volume on the cluster
func GetIsiVolume(
	ctx context.Context,
	client api.Client,
	name string) (resp *getIsiVolumeAttributesResp, err error) {

	// PAPI call: GET https://1.2.3.4:8080/namespace/path/to/volume/?metadata
	err = client.Get(
		ctx,
		realNamespacePath(client),
		name,
		metadataQS,
		nil,
		&resp)
	return resp, err
}

// DeleteIsiVolume removes a volume from the cluster
func DeleteIsiVolume(
	ctx context.Context,
	client api.Client,
	name string) (resp *getIsiVolumesResp, err error) {

	err = client.Delete(
		ctx,
		realNamespacePath(client),
		name,
		recursiveTrueQS,
		nil,
		&resp)
	return resp, err
}

// CopyIsiVolume creates a new volume on the cluster based on an existing volume
func CopyIsiVolume(
	ctx context.Context,
	client api.Client,
	sourceName, destinationName string) (resp *getIsiVolumesResp, err error) {
	// PAPI calls: PUT https://1.2.3.4:8080/namespace/path/to/volumes/destination_volume_name
	//             x-isi-ifs-copy-source: /path/to/volumes/source_volume_name

	// copy the volume
	err = client.Put(
		ctx,
		realNamespacePath(client),
		destinationName,
		nil,
		map[string]string{
			"x-isi-ifs-copy-source": path.Join(
				"/",
				realNamespacePath(client),
				sourceName),
		},
		nil,
		&resp)
	return resp, err
}
