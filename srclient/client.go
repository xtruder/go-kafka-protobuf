//go:generate go run github.com/golang/mock/mockgen -source=client.go -destination=client_mock_test.go -package=srclient -self_package=.

package srclient

import (
	"context"
	"regexp"
)

// SchemaType defines type of schema
type SchemaType string

func (s SchemaType) String() string {
	return string(s)
}

const (
	// ProtobufSchemaType schema type
	ProtobufSchemaType SchemaType = "PROTOBUF"

	// AvroSchemaType schema type
	AvroSchemaType SchemaType = "AVRO"

	// JSONSchemaType scema type
	JSONSchemaType SchemaType = "JSON"
)

/*Reference defines struct for schema registry references

In case of protobuf these are imported schema files and in case
of JSON schema these are schemas referenced using $ref*/
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Schema is a data structure that holds all
// the relevant information about schemas.
type Schema struct {
	ID         int         `json:"id,omitempty"`
	Schema     string      `json:"schema,omitempty"`
	Subject    string      `json:"subject,omitempty"`
	Version    int         `json:"version,omitempty"`
	References []Reference `json:"references,omitempty"`
	Type       SchemaType  `json:"schemaType,omitempty"`
}

func (s *Schema) GetRawSchema() (rawSchema string) {
	switch s.Type {
	case AvroSchemaType, JSONSchemaType:
		compiledRegex := regexp.MustCompile(`\r?\n`)
		rawSchema = compiledRegex.ReplaceAllString(s.Schema, " ")
	case ProtobufSchemaType:
		fallthrough
	default:
		rawSchema = s.Schema
	}

	return
}

// Client defines interface for schema registry client, that
// is implemented by HTTPClient and CachingClient
type Client interface {
	GetSubjects(ctx context.Context) ([]string, error)
	GetSubjectVersions(ctx context.Context, subject string) ([]int, error)
	GetSchemaByID(ctx context.Context, schemaID int) (*Schema, error)
	GetSchemaByVersion(ctx context.Context, subject string, version int) (*Schema, error)
	GetSchemaSubjectVersions(ctx context.Context, schemaID int) (map[string]int, error)
	GetLatestSchema(ctx context.Context, subject string) (*Schema, error)
	CreateSchema(ctx context.Context, schema *Schema) (*Schema, error)
	DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error)
	DeleteSchemaByVersion(ctx context.Context, subject string, version int, permanent bool) (int, error)
	IsSchemaCompatible(ctx context.Context, schema *Schema) (bool, error)
}

// Option interface is here, so we can type check if valid arg is parsed as Option to client
type Option interface{ OptionType() }

type globalOptions struct {
	enableCaching bool
}

type GlobalOption func(o *globalOptions)

func (GlobalOption) OptionType() {}

// WithCaching enables client caching
func WithCaching(enable ...bool) GlobalOption {
	return func(o *globalOptions) {
		o.enableCaching = enableOpt(enable)
	}
}

// NewClient creates a new Client with optional caching
func NewClient(opts ...Option) Client {
	globals := &globalOptions{}
	baseClientOpts := []BaseClientOption{}
	cachingClientOpts := []CachingClientOption{}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case GlobalOption:
			opt(globals)
		case BaseClientOption:
			baseClientOpts = append(baseClientOpts, opt)
		case CachingClientOption:
			cachingClientOpts = append(cachingClientOpts, opt)
		}
	}

	baseClient := NewBaseClient(baseClientOpts...)

	if globals.enableCaching {
		return NewCachingClient(baseClient, cachingClientOpts...)
	}

	return baseClient
}
