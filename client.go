package goisilon

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/tenortim/goisilon/api"
)

// Client is an Isilon client.
type Client struct {

	// API is the underlying OneFS API client.
	API api.Client
}

// NewClient returns a new Isilon client struct initialized from the environment.
func NewClient(ctx context.Context) (*Client, error) {
	insecure, _ := strconv.ParseBool(os.Getenv("GOISILON_INSECURE"))
	return NewClientWithArgs(
		ctx,
		os.Getenv("GOISILON_ENDPOINT"),
		insecure,
		os.Getenv("GOISILON_USERNAME"),
		os.Getenv("GOISILON_GROUP"),
		os.Getenv("GOISILON_PASSWORD"),
		os.Getenv("GOISILON_VOLUMEPATH"))
}

// NewClientWithArgs returns a new Isilon client struct initialized from the supplied arguments.
func NewClientWithArgs(
	ctx context.Context,
	endpoint string,
	insecure bool,
	user, group, pass, volumesPath string) (*Client, error) {

	timeout, _ := time.ParseDuration(os.Getenv("GOISILON_TIMEOUT"))

	client, err := api.New(
		ctx, endpoint, user, pass, group,
		&api.ClientOptions{
			Insecure:    insecure,
			VolumesPath: volumesPath,
			Timeout:     timeout,
		})
	if err != nil {
		return nil, err
	}

	return &Client{client}, err
}
