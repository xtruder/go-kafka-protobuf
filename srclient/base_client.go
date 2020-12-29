package srclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

const (
	urlSchemaByID             = urlPath("/schemas/ids/%s")
	urlSchemaByIDVersions     = urlPath("/schemas/ids/%s/versions")
	urlSubjects               = urlPath("/subjects")
	urlSubjectSchemaByVersion = urlPath("/subjects/%s/versions/%s")
	urlSubject                = urlPath("/subjects/%s")
	urlSubjectVersions        = urlPath("/subjects/%s/versions")
	urlSchemaCompatibility    = urlPath("/compatibility/subjects/%s/versions/%s")
)

var ErrNotFound = errors.New("404 not found")

const contentType = "application/vnd.schemaregistry.v1+json"

type schemaRequest struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType"`
	References []Reference `json:"references"`
}

func schemaRequestFromSchema(schema *Schema) (request schemaRequest) {
	request.Schema = schema.GetRawSchema()
	request.SchemaType = schema.Type.String()
	request.References = schema.References
	return
}

type credentials struct {
	username string
	password string
}

type BaseClientOption func(*BaseClient)

func (BaseClientOption) OptionType() {}

// WithURL option sets URL for client
func WithURL(val interface{}) BaseClientOption {
	var u *url.URL
	var err error

	switch v := val.(type) {
	case string:
		if u, err = url.Parse(v); err != nil {
			panic(fmt.Errorf("schema registry url is invalid '%s': %w", v, err))
		}

		if u.Hostname() == "" {
			panic(fmt.Errorf("schema registry url is missing hostname: '%s'", v))
		}
	case *url.URL:
		u = v
	default:
		panic(fmt.Errorf("schema registry url is of invalid type: %s", reflect.TypeOf(u).String()))
	}

	return func(c *BaseClient) {
		c.url = u
	}
}

// WithInsecure overrides default transport and sets insecure skip verify
func WithInsecure(insecure ...bool) BaseClientOption {
	return func(c *BaseClient) {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: enableOpt(insecure)},
		}

		c.httpClient.Transport = tr
	}
}

// WithCredentials option sets credentials for client
func WithCredentials(username string, password string) BaseClientOption {
	return func(c *BaseClient) {
		c.credentials = &credentials{username, password}
	}
}

// WithHTTPClient option sets http client to use for requests
func WithHTTPClient(httpClient *http.Client) BaseClientOption {
	if httpClient == nil {
		panic(fmt.Errorf("no http client provided"))
	}

	return func(c *BaseClient) {
		c.httpClient = httpClient
	}
}

var defaultBaseClientOpts = []BaseClientOption{
	WithURL("http://localhost:8081"),
}

// BaseClient defines schema registry http client
type BaseClient struct {
	httpClient *http.Client

	url         *url.URL
	credentials *credentials
}

// NewBaseClient creates new HTTP client
func NewBaseClient(opts ...BaseClientOption) *BaseClient {
	c := &BaseClient{
		httpClient: &http.Client{},
	}

	for _, opt := range defaultBaseClientOpts {
		opt(c)
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// GetSubjects method gets list of defined subjects
func (c *BaseClient) GetSubjects(ctx context.Context) ([]string, error) {
	allSubjects := []string{}
	err := c.jsonRequest(ctx, "GET", urlSubjects, nil, &allSubjects)
	if err != nil {
		return nil, fmt.Errorf("error getting subjects: %w", err)
	}

	return allSubjects, nil
}

func (c *BaseClient) GetSubjectVersions(ctx context.Context, subject string) ([]int, error) {
	versions := []int{}
	err := c.jsonRequest(ctx, "GET", urlSubjectVersions.Format(subject), nil, &versions)
	if err != nil {
		return nil, fmt.Errorf("error getting schema versions: %w", err)
	}

	return versions, nil
}

func (c *BaseClient) GetSchemaByID(ctx context.Context, schemaID int) (*Schema, error) {
	schema := &Schema{}
	err := c.jsonRequest(ctx, "GET", urlSchemaByID.Format(schemaID), nil, schema)
	if err != nil {
		return nil, fmt.Errorf("error getting schema by id: %w", err)
	}

	// update schemaID
	schema.ID = schemaID

	return schema, nil
}

func (c *BaseClient) GetLatestSchema(ctx context.Context, subject string) (*Schema, error) {
	return c.getSchemaByVersion(ctx, subject, "latest")
}

func (c *BaseClient) GetSchemaByVersion(ctx context.Context, subject string, version int) (*Schema, error) {
	return c.getSchemaByVersion(ctx, subject, strconv.Itoa(version))
}

func (c *BaseClient) CreateSchema(ctx context.Context, schema *Schema) (*Schema, error) {
	type createSchemaResponse struct {
		ID int `json:"id"`
	}

	schemaReq := schemaRequestFromSchema(schema)
	createSchemaResp := &createSchemaResponse{}
	if err := c.jsonRequest(ctx, "POST", urlSubjectVersions.Format(schema.Subject), schemaReq, createSchemaResp); err != nil {
		return nil, fmt.Errorf("error creating schema: %w", err)
	}

	result := *schema

	// set schema ID
	result.ID = createSchemaResp.ID

	// get updated schema version
	versions, err := c.getSchemaSubjectVersions(ctx, createSchemaResp.ID)
	if err != nil {
		return nil, err
	}

	// set updated schema version
	result.Version, _ = versions[result.Subject]

	return &result, nil
}

func (c *BaseClient) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	url := urlSubject.Format(subject)

	resp := []int{}
	err := c.jsonRequest(ctx, "DELETE", url, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("error deleting subject: %w", err)
	}

	if permanent {
		err := c.jsonRequest(ctx, "DELETE", url+"?permanent=true", nil, nil)
		if err != nil {
			return nil, fmt.Errorf("error making subject delete permanent: %w", err)
		}
	}

	return resp, nil
}

func (c *BaseClient) DeleteSchemaByVersion(ctx context.Context, subject string, version int, permanent bool) (int, error) {
	url := urlSubjectSchemaByVersion.Format(subject, version)

	resp := 0
	err := c.jsonRequest(ctx, "DELETE", url, nil, &resp)
	if err != nil {
		return 0, fmt.Errorf("error deleting schema version: %w", err)
	}

	if permanent {
		err := c.jsonRequest(ctx, "DELETE", url+"?permanent=true", nil, nil)
		if err != nil {
			return 0, fmt.Errorf("error making schema version delete permanent: %w", err)
		}
	}

	return resp, nil
}

func (c *BaseClient) IsSchemaCompatible(ctx context.Context, schema *Schema) (bool, error) {
	type response struct {
		IsCompatible bool `json:"is_compatible"`
	}

	schemaReq := schemaRequestFromSchema(schema)
	uri := urlSchemaCompatibility.Format(schema.Subject, schema.Version)

	resp := &response{}

	err := c.jsonRequest(ctx, "POST", uri, schemaReq, resp)
	if err != nil {
		return false, fmt.Errorf("error checking schema compatibility: %w", err)
	}

	return resp.IsCompatible, nil
}

func (c *BaseClient) GetSchemaSubjectVersions(ctx context.Context, schemaID int) (map[string]int, error) {
	return c.getSchemaSubjectVersions(ctx, schemaID)
}

func (c *BaseClient) getSchemaByVersion(ctx context.Context, subject string, version string) (*Schema, error) {
	resp := &Schema{}

	err := c.jsonRequest(ctx, "GET", urlSubjectSchemaByVersion.Format(subject, version), nil, resp)
	if err != nil {
		return nil, fmt.Errorf("error getting schema by version: %w", err)
	}

	// get the actual version
	if version == "latest" {
		versions, err := c.getSchemaSubjectVersions(ctx, resp.ID)
		if err != nil {
			return nil, err
		}

		resp.Version, _ = versions[resp.Subject]
	} else {
		resp.Version, _ = strconv.Atoi(version)
	}

	return resp, nil
}

func (c *BaseClient) getSchemaSubjectVersions(ctx context.Context, schemaID int) (map[string]int, error) {
	result := map[string]int{}

	type subjectVersions struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}

	resp := []subjectVersions{}
	err := c.jsonRequest(ctx, "GET", urlSchemaByIDVersions.Format(schemaID), nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("error getting schema by id version: %w", err)
	}

	for _, elem := range resp {
		result[elem.Subject] = elem.Version
	}

	return result, nil
}

func (c *BaseClient) httpRequest(ctx context.Context, method string, path urlPath, payload io.Reader) ([]byte, error) {
	// construct full url
	url := strings.TrimRight(c.url.String(), "/") + "/" + strings.TrimLeft(string(path), "/")

	req, err := http.NewRequestWithContext(ctx, string(method), url, payload)
	if err != nil {
		return nil, err
	}

	if c.credentials != nil {
		req.SetBasicAuth(c.credentials.username, c.credentials.password)
	}

	req.Header.Set("Content-Type", contentType)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, createHTTPError(resp)
	}

	return ioutil.ReadAll(resp.Body)
}

func (c *BaseClient) jsonRequest(ctx context.Context, method string, path urlPath, req interface{}, resp interface{}) error {
	var payload io.Reader
	if req != nil {
		data, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("error marshaling json http payload: %w", err)
		}

		payload = bytes.NewBuffer(data)
	}

	r, err := c.httpRequest(ctx, method, path, payload)
	if err != nil {
		return err
	}

	if resp != nil {
		if err = json.Unmarshal(r, resp); err != nil {
			return fmt.Errorf("error unmarshalling json http response: %w", err)
		}
	}

	return nil
}

func createHTTPError(resp *http.Response) error {
	decoder := json.NewDecoder(resp.Body)

	var errorResp struct {
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
	}

	if err := decoder.Decode(&errorResp); err != nil {
		return fmt.Errorf("%s", resp.Status)

	}

	if resp.StatusCode == 404 {
		return fmt.Errorf("%w: %s", ErrNotFound, errorResp.Message)
	}

	return fmt.Errorf("%s: %s", resp.Status, errorResp.Message)
}
