package srclient

import (
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type makeSchemaOpt func(s *Schema)

func withRandomSubject(s *Schema) {
	s.Subject = randomString(5)
}

func withRandomID(s *Schema) {
	s.ID = rand.Intn(100)
}

func withRandomVersion(s *Schema) {
	s.Version = rand.Intn(100)
}

func withTestReferences(s *Schema) {
	s.References = []Reference{
		{
			Name:    "test",
			Subject: "test",
			Version: 1,
		},
	}
}

func withRandomSchema(s *Schema) {
	s.Type = ProtobufSchemaType
	s.Schema = fmt.Sprintf(`
		syntax = "proto3";

		message %s {
			string key = 1;
		}
	`, randomString(5))
}

func makeSchema(opts ...makeSchemaOpt) *Schema {
	schema := &Schema{
		Type:    ProtobufSchemaType,
		Subject: "subject1",
		Schema: `
			syntax = "proto3";

			package go_kafka_protobuf;

			message Test {
				string name = 1;
				string value = 2;
			}
		`,
	}

	for _, opt := range opts {
		opt(schema)
	}

	return schema
}

func TestNewClient(t *testing.T) {
	c := NewClient(
		WithCaching(),
		WithExpiration(time.Minute),
		WithURL("https://localhost:8081"),
		WithInsecure(),
	)

	require.IsType(t, &CachingClient{}, c)

	cachingClient := c.(*CachingClient)

	require.NotNil(t, cachingClient.cache.cache)
	require.NotNil(t, cachingClient.cache.infcache)

	require.IsType(t, &BaseClient{}, cachingClient.client)

	baseClient := cachingClient.client.(*BaseClient)
	require.NotNil(t, baseClient.httpClient)

	require.IsType(t, &http.Transport{}, baseClient.httpClient.Transport)
	require.True(t, baseClient.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify)
}
