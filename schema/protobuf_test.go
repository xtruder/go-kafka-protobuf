package schema

import (
	"testing"

	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/require"
	"github.com/xtruder/go-kafka-protobuf/proto"
)

func TestProtobufSchemaRegistrator(t *testing.T) {
	client := srclient.CreateSchemaRegistryClient("http://schema-registry:8081")
	registrator := NewProtobufSchemaRegistrator(client)
	err := registrator.Register("user-value", &proto.User{})

	require.NoError(t, err)
}
