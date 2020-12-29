package protobuf

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/jhump/protoreflect/desc"
)

/* toMessageIndices converts message name to message indices

It does so by walking tree of nested messages in proto file descriptor
and getting indexes in each level. this code is based on:
https://github.com/confluentinc/schema-registry/blob/97667/protobuf-provider/src/main/java/io/confluent/kafka/schemaregistry/protobuf/ProtobufSchema.java#L876
*/
func toMessageIndices(desc *desc.FileDescriptor, name string) []int {
	indexes := []int{}
	parts := strings.Split(name, ".")

	messageTypes := desc.GetMessageTypes()
	for _, part := range parts {
		for i, mt := range messageTypes {
			if mt.GetName() == part {
				indexes = append(indexes, i)
				messageTypes = mt.GetNestedMessageTypes()
				break
			}
		}
	}

	return indexes
}

func msgIndicesToByteArray(indices []int) (result []byte) {
	encoded := make([]byte, binary.MaxVarintLen32)

	n := binary.PutVarint(encoded, int64(len(indices)))
	result = append(result, encoded[:n]...)
	for _, index := range indices {
		n = binary.PutVarint(encoded, int64(index))
		result = append(result, encoded[:n]...)
	}

	return result
}

func byteArrayToMsgIndices(bytes []byte) (indices []int, total int, err error) {
	count, n := binary.Varint(bytes)

	if n <= 0 {
		err = fmt.Errorf("error decoding indices: invalid size")
		return
	}

	total += n
	for i := 0; i < int(count); i++ {
		idx, n := binary.Varint(bytes[total:])

		if n <= 0 {
			err = fmt.Errorf("error decoding indices: invalid size")
			return
		}

		total += n
		indices = append(indices, int(idx))
	}

	return
}
