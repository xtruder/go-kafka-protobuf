package srclient

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

var _ Client = (*BaseClient)(nil)

func newTestBaseClient() *BaseClient {
	url := os.Getenv("SCHEMA_REGISTRY_URL")

	if url == "" {
		panic("SCHEMA_REGISTRY_URL url not set")
	}

	return NewBaseClient(WithURL(url))
}

func skipIntegration(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("Skipping integration tests")
	}
}

func TestBaseClient(t *testing.T) {
	c := NewBaseClient(
		WithURL("https://localhost:8081"),
		WithInsecure(),
		WithCredentials("user", "pass"),
	)

	require.IsType(t, &BaseClient{}, c)
}

func TestGetSubjects(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject)

	_, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	subjects, err := c.GetSubjects(context.Background())
	require.NoError(t, err)
	require.Contains(t, subjects, schema.Subject)
}

func TestGetSubjectVersions(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject)

	_, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	schema.Schema = schema.Schema + `
		message Foo {
			string name = 1;
		}
	`

	_, err = c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	versions, err := c.GetSubjectVersions(context.Background(), schema.Subject)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, versions)
}

func TestGetSchemaByID(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	result, err := c.GetSchemaByID(context.Background(), schema.ID)
	require.NoError(t, err)
	require.Equal(t, schema.ID, result.ID)
	require.Equal(t, schema.Type, result.Type)
	require.Equal(t, schema.References, result.References)
}

func TestGetSchemaByIDNotFound(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	_, err := c.GetSchemaByID(context.Background(), 999999)

	require.True(t, errors.Is(err, ErrNotFound))
}

func TestGetLatestSchema(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	result, err := c.GetLatestSchema(context.Background(), schema.Subject)
	require.NoError(t, err)
	require.Equal(t, schema.Version, result.Version)
}

func TestGetSchemaByVersion(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	result, err := c.GetSchemaByVersion(context.Background(), schema.Subject, schema.Version)
	require.NoError(t, err)
	require.Equal(t, schema.Version, result.Version)
}

func TestGetSchemaSubjectVersions(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject, withRandomSchema)

	schema1, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	schema.Subject = randomString(5)

	schema2, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	require.Equal(t, schema1.ID, schema2.ID)

	versions, err := c.GetSchemaSubjectVersions(context.Background(), schema1.ID)
	require.NoError(t, err)
	require.Equal(t, map[string]int{schema1.Subject: schema1.Version, schema2.Subject: schema2.Version}, versions)
}

func TestDeleteSubject(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject, withRandomSchema)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	versions, err := c.DeleteSubject(context.Background(), schema.Subject, false)
	require.NoError(t, err)
	require.Equal(t, []int{schema.Version}, versions)

	subjects, err := c.GetSubjects(context.Background())
	require.NoError(t, err)
	require.NotContains(t, subjects, schema.Subject)

	_, err = c.GetSchemaByID(context.Background(), schema.ID)
	require.NoError(t, err)
}

func TestDeleteSubjectPermanent(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject, withRandomSchema)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	versions, err := c.DeleteSubject(context.Background(), schema.Subject, true)
	require.NoError(t, err)
	require.Equal(t, []int{schema.Version}, versions)

	subjects, err := c.GetSubjects(context.Background())
	require.NoError(t, err)
	require.NotContains(t, subjects, schema.Subject)

	_, err = c.GetSchemaByID(context.Background(), schema.ID)
	require.True(t, errors.Is(err, ErrNotFound))
}

func TestDeleteSchemaSubjectVersion(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject, withRandomSchema)

	schema1, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	schema.Schema = schema.Schema + `
		message Foo {
			string name = 1;
		}
	`

	schema2, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	version, err := c.DeleteSchemaByVersion(context.Background(), schema.Subject, schema1.Version, false)
	require.NoError(t, err)
	require.Equal(t, schema1.Version, version)

	_, err = c.GetSchemaByID(context.Background(), schema1.ID)
	require.NoError(t, err)

	versions, err := c.GetSubjectVersions(context.Background(), schema.Subject)
	require.NoError(t, err)
	require.Equal(t, []int{schema2.Version}, versions)
}

func TestDeleteSchemaSubjectVersionPermanent(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject, withRandomSchema)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	_, err = c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	version, err := c.DeleteSchemaByVersion(context.Background(), schema.Subject, schema.Version, true)
	require.NoError(t, err)
	require.Equal(t, schema.Version, version)

	_, err = c.GetSchemaByID(context.Background(), schema.ID)
	require.True(t, errors.Is(err, ErrNotFound))
}

func TestSchemaCompatible(t *testing.T) {
	skipIntegration(t)

	c := newTestBaseClient()

	schema := makeSchema(withRandomSubject, withRandomSchema)

	schema, err := c.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	compatible, err := c.IsSchemaCompatible(context.Background(), schema)
	require.NoError(t, err)
	require.True(t, compatible)
}
