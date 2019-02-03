package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	log "github.com/akutz/gournal"

	"github.com/tenortim/goisilon/api/json"
)

const (
	headerKeyContentType                  = "Content-Type"
	headerValContentTypeJSON              = "application/json"
	headerValContentTypeBinaryOctetStream = "binary/octet-stream"
	defaultVolumesPath                    = "/ifs/volumes"
)

var (
	debug, _     = strconv.ParseBool(os.Getenv("GOISILON_DEBUG"))
	errNewClient = errors.New("missing endpoint, username, or password")
)

// Client is an API client.
type Client interface {

	// Do sends an HTTP request to the OneFS API.
	Do(
		ctx context.Context,
		method, path, id string,
		params OrderedValues,
		body, resp interface{}) error

	// DoWithHeaders sends an HTTP request to the OneFS API.
	DoWithHeaders(
		ctx context.Context,
		method, path, id string,
		params OrderedValues, headers map[string]string,
		body, resp interface{}) error

	// Get sends an HTTP request using the GET method to the OneFS API.
	Get(
		ctx context.Context,
		path, id string,
		params OrderedValues, headers map[string]string,
		resp interface{}) error

	// Post sends an HTTP request using the POST method to the OneFS API.
	Post(
		ctx context.Context,
		path, id string,
		params OrderedValues, headers map[string]string,
		body, resp interface{}) error

	// Put sends an HTTP request using the PUT method to the OneFS API.
	Put(
		ctx context.Context,
		path, id string,
		params OrderedValues, headers map[string]string,
		body, resp interface{}) error

	// Delete sends an HTTP request using the DELETE method to the OneFS API.
	Delete(
		ctx context.Context,
		path, id string,
		params OrderedValues, headers map[string]string,
		resp interface{}) error

	// APIVersion returns the API version.
	APIVersion() uint8

	// User returns the user name used to access the OneFS API.
	User() string

	// Group returns the group name used to access the OneFS API.
	Group() string

	// VolumesPath returns the client's configured volumes path.
	VolumesPath() string

	// VolumePath returns the path to a volume with the provided name.
	VolumePath(name string) string
}

type client struct {
	http            *http.Client
	hostname        string
	username        string
	groupname       string
	password        string
	volumePath      string
	apiVersion      uint8
	apiMinorVersion uint8
}

type apiVerResponse struct {
	Latest *string `json:"latest"`
}

// Error is an API error.
type Error struct {
	Code    string `json:"code"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

// JSONError is a JSON response with one or more errors.
type JSONError struct {
	StatusCode int
	Err        []Error `json:"errors"`
}

// ClientOptions are options for the API client.
type ClientOptions struct {
	// Insecure is a flag that indicates whether or not to supress SSL errors.
	Insecure bool

	// VolumesPath is the location on the Isilon server where volumes are
	// stored.
	VolumesPath string

	// Timeout specifies a time limit for requests made by this client.
	Timeout time.Duration
}

// New returns a new API client.
func New(
	ctx context.Context,
	hostname, username, password, groupname string,
	opts *ClientOptions) (Client, error) {

	if hostname == "" || username == "" || password == "" {
		return nil, errNewClient
	}

	c := &client{
		hostname:   hostname,
		username:   username,
		groupname:  groupname,
		password:   password,
		volumePath: defaultVolumesPath,
	}

	c.http = &http.Client{}

	if opts != nil {
		if opts.VolumesPath != "" {
			c.volumePath = opts.VolumesPath
		}

		if opts.Timeout != 0 {
			c.http.Timeout = opts.Timeout
		}

		if opts.Insecure {
			c.http.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			}
		}
	}

	resp := &apiVerResponse{}
	if err := c.Get(ctx, "/platform/latest", "", nil, nil, resp); err != nil &&
		!strings.HasPrefix(err.Error(), "json: ") {
		return nil, err
	}

	if resp.Latest != nil {
		s := *resp.Latest
		c.apiMinorVersion = 0
		if i := strings.Index(s, "."); i != -1 {
			ms := s[i+1:]
			m, err := strconv.ParseUint(ms, 10, 8)
			if err != nil {
				return nil, err
			}
			c.apiMinorVersion = uint8(m)
			s = s[:i]
		}
		i, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return nil, err
		}
		c.apiVersion = uint8(i)
	} else {
		c.apiVersion = 2
	}

	if c.apiVersion < 3 {
		return nil, errors.New("OneFS releases older than 8.0 are no longer supported")
	}

	return c, nil
}

func (c *client) Get(
	ctx context.Context,
	path, id string,
	params OrderedValues, headers map[string]string,
	resp interface{}) error {

	return c.DoWithHeaders(
		ctx, http.MethodGet, path, id, params, headers, nil, resp)
}

func (c *client) Post(
	ctx context.Context,
	path, id string,
	params OrderedValues, headers map[string]string,
	body, resp interface{}) error {

	return c.DoWithHeaders(
		ctx, http.MethodPost, path, id, params, headers, body, resp)
}

func (c *client) Put(
	ctx context.Context,
	path, id string,
	params OrderedValues, headers map[string]string,
	body, resp interface{}) error {

	return c.DoWithHeaders(
		ctx, http.MethodPut, path, id, params, headers, body, resp)
}

func (c *client) Delete(
	ctx context.Context,
	path, id string,
	params OrderedValues, headers map[string]string,
	resp interface{}) error {

	return c.DoWithHeaders(
		ctx, http.MethodDelete, path, id, params, headers, nil, resp)
}

func (c *client) Do(
	ctx context.Context,
	method, path, id string,
	params OrderedValues,
	body, resp interface{}) error {

	return c.DoWithHeaders(ctx, method, path, id, params, nil, body, resp)
}

func beginsWithSlash(s string) bool {
	return s[0] == '/'
}

func endsWithSlash(s string) bool {
	return s[len(s)-1] == '/'
}

func (c *client) DoWithHeaders(
	ctx context.Context,
	method, uri, id string,
	params OrderedValues, headers map[string]string,
	body, resp interface{}) error {

	res, isDebugLog, err := c.DoAndGetResponseBody(
		ctx, method, uri, id, params, headers, body)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if isDebugLog {
		logResponse(ctx, res)
	}

	// parse the response
	switch {
	case res == nil:
		return nil
	case res.StatusCode >= 200 && res.StatusCode <= 299:
		if resp == nil {
			return nil
		}
		dec := json.NewDecoder(res.Body)
		if err = dec.Decode(resp); err != nil && err != io.EOF {
			return err
		}
	default:
		return parseJSONError(res)
	}

	return nil
}

func (c *client) DoAndGetResponseBody(
	ctx context.Context,
	method, uri, id string,
	params OrderedValues, headers map[string]string,
	body interface{}) (*http.Response, bool, error) {

	var (
		err                   error
		req                   *http.Request
		res                   *http.Response
		ubf                   = &bytes.Buffer{}
		lid                   = len(id)
		luri                  = len(uri)
		hostnameEndsWithSlash = endsWithSlash(c.hostname)
		uriBeginsWithSlash    = beginsWithSlash(uri)
		uriEndsWithSlash      = endsWithSlash(uri)
	)

	ubf.WriteString(c.hostname)

	if !hostnameEndsWithSlash && (luri > 0 || lid > 0) {
		ubf.WriteString("/")
	}

	if luri > 0 {
		if uriBeginsWithSlash {
			ubf.WriteString(uri[1:])
		} else {
			ubf.WriteString(uri)
		}
		if !uriEndsWithSlash {
			ubf.WriteString("/")
		}
	}

	if lid > 0 {
		ubf.WriteString(id)
	}

	// add parameters to the URI
	if len(params) > 0 {
		ubf.WriteByte('?')
		if err := params.EncodeTo(ubf); err != nil {
			return nil, false, err
		}
	}

	u, err := url.Parse(ubf.String())
	if err != nil {
		return nil, false, err
	}

	var isContentTypeSet bool

	// marshal the message body (assumes json format)
	if body != nil {
		if r, ok := body.(io.ReadCloser); ok {
			req, err = http.NewRequest(method, u.String(), r)
			defer r.Close()
			if v, ok := headers[headerKeyContentType]; ok {
				req.Header.Set(headerKeyContentType, v)
			} else {
				req.Header.Set(
					headerKeyContentType, headerValContentTypeBinaryOctetStream)
			}
			isContentTypeSet = true
		} else {
			buf := &bytes.Buffer{}
			enc := json.NewEncoder(buf)
			if err = enc.Encode(body); err != nil {
				return nil, false, err
			}
			req, err = http.NewRequest(method, u.String(), buf)
			if v, ok := headers[headerKeyContentType]; ok {
				req.Header.Set(headerKeyContentType, v)
			} else {
				req.Header.Set(headerKeyContentType, headerValContentTypeJSON)
			}
			isContentTypeSet = true
		}
	} else {
		req, err = http.NewRequest(method, u.String(), nil)
	}

	if err != nil {
		return nil, false, err
	}

	if !isContentTypeSet {
		isContentTypeSet = req.Header.Get(headerKeyContentType) != ""
	}

	// add headers to the request
	if len(headers) > 0 {
		for header, value := range headers {
			if header == headerKeyContentType && isContentTypeSet {
				continue
			}
			req.Header.Add(header, value)
		}
	}

	// set the username and password
	req.SetBasicAuth(c.username, c.password)

	var (
		isDebugLog bool
		logReqBuf  = &bytes.Buffer{}
	)

	if lvl, ok := ctx.Value(
		log.LevelKey()).(log.Level); ok && lvl >= log.DebugLevel {
		isDebugLog = true
	}

	logRequest(ctx, logReqBuf, req)
	if isDebugLog {
		log.Debug(ctx, logReqBuf.String())
	}

	// send the request
	req = req.WithContext(ctx)
	if res, err = c.http.Do(req); err != nil {
		if !isDebugLog {
			log.Debug(ctx, logReqBuf.String())
		}
		return nil, isDebugLog, err
	}

	return res, isDebugLog, err
}

func (c *client) APIVersion() uint8 {
	return c.apiVersion
}

func (c *client) User() string {
	return c.username
}

func (c *client) Group() string {
	return c.groupname
}

func (c *client) VolumesPath() string {
	return c.volumePath
}

func (c *client) VolumePath(volumeName string) string {
	return path.Join(c.volumePath, volumeName)
}

func (err *JSONError) Error() string {
	return err.Err[0].Message
}

func parseJSONError(r *http.Response) error {
	jsonError := &JSONError{}
	if err := json.NewDecoder(r.Body).Decode(jsonError); err != nil {
		return err
	}

	jsonError.StatusCode = r.StatusCode
	if jsonError.Err[0].Message == "" {
		jsonError.Err[0].Message = r.Status
	}

	return jsonError
}
