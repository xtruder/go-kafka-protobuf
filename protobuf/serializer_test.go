package protobuf

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/xtruder/go-kafka-protobuf/protobuf/fixture"
)

func TestSerializeMessage(t *testing.T) {
	tests := []struct {
		name     string
		indices  []int
		schemaID int
		msg      proto.Message
	}{
		{
			name:     "simple messsage",
			indices:  []int{0},
			schemaID: 1,
			msg: &fixture.User{
				Id: "test",
			},
		},
		{
			name:     "nested message",
			indices:  []int{0, 0},
			schemaID: 2,
			msg: &fixture.User_Address{
				Street:     "Kolodvorska 46",
				PostalCode: "1218",
				City:       "Ljubljana",
				Country:    "Slovenia",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := serializeMessage(test.schemaID, test.msg)
			require.NoError(t, err)

			require.Equal(t, uint8(magicByte), result[0], "invalid magic byte")
			rest := result[1:]

			schemaID := binary.BigEndian.Uint32(rest)
			require.Equal(t, uint32(test.schemaID), schemaID, "invalid schema id")
			rest = rest[4:]

			indices, total, err := byteArrayToMsgIndices(rest)
			require.NoError(t, err)
			require.EqualValues(t, test.indices, indices, "invalid message indexes")
			rest = rest[total:]

			resultMsg := reflect.New(reflect.ValueOf(test.msg).Elem().Type()).Interface().(proto.Message)
			err = proto.Unmarshal(rest, resultMsg)
			require.NoError(t, err)

			require.True(t, proto.Equal(test.msg, resultMsg), "proto messages not equal")
		})
	}
}
