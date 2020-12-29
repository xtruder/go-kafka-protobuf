package protobuf

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
)

const magicByte = 0

// ProtoSerDe implements protbuf messages serializer and deserializer
type ProtoSerDe struct{}

func NewProtoSerDe() *ProtoSerDe {
	return &ProtoSerDe{}
}

func (s *ProtoSerDe) Serialize(schemaID int, msg interface{}) ([]byte, error) {
	protoMsg, ok := msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid message type: must be of proto.Message")
	}

	return serializeMessage(schemaID, protoMsg)
}

func (s *ProtoSerDe) Deserialize(msgData []byte, msg interface{}) (int, error) {
	schemaID, _, msgData, err := parseMessage(msgData)
	if err != nil {
		return 0, err
	}

	switch m := msg.(type) {
	case proto.Message:
		return schemaID, deserializeMessageIntoProto(msgData, m)
	default:
		return 0, fmt.Errorf("invalid deserialize type: %s", reflect.TypeOf(m).String())
	}
}

func deserializeMessageIntoProto(data []byte, msg proto.Message) error {
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("error unmarshaling proto: %w", err)
	}

	return nil
}

func serializeMessage(schemaID int, msg proto.Message) ([]byte, error) {
	msgDesc, err := desc.LoadMessageDescriptorForMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("error loading message desciprot for message: %w", err)
	}

	fileDesc := msgDesc.GetFile()
	indices := toMessageIndices(fileDesc, msgDesc.GetFullyQualifiedName())

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))

	msgIdxBytes := msgIndicesToByteArray(indices)

	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("error marshalling protobuf message: %w", err)
	}

	messageValue := []byte{magicByte}
	messageValue = append(messageValue, schemaIDBytes...)
	messageValue = append(messageValue, msgIdxBytes...)
	messageValue = append(messageValue, msgBytes...)

	return messageValue, nil
}

func parseMessage(data []byte) (schemaID int, indices []int, msg []byte, err error) {
	rest := data

	if rest[0] != magicByte {
		err = fmt.Errorf("error parsing message: invalid magic byte '%d, must be '%d'", rest[0], magicByte)
		return
	}

	rest = rest[1:]

	if len(rest) < 4 {
		err = fmt.Errorf("errror parsing message: cannot read schemaID, missing data")
		return
	}

	schemaID = int(binary.BigEndian.Uint32(rest[:4]))
	rest = rest[4:]

	var idxBytes int
	if indices, idxBytes, err = byteArrayToMsgIndices(rest); err != nil {
		return
	}

	msg = rest[idxBytes:]

	return
}
