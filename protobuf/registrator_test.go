package protobuf

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xtruder/go-kafka-protobuf/protobuf/fixture"
	"github.com/xtruder/go-kafka-protobuf/srclient"
)

func TestProtobufSchemaRegistrator(t *testing.T) {
	client := srclient.NewClient(srclient.WithURL("http://schema-registry:8081"))
	registrator := NewSchemaRegistrator(client)
	id, err := registrator.RegisterValue(context.Background(), "user-value", &fixture.User{})
	require.NoError(t, err)

	_, err = registrator.Load(context.Background(), id, "schema.proto")
	require.NoError(t, err)
}
