package gokafkaproto

type SchemaRegistrator interface {
	RegisterKey(topic string, msg interface{}) error
	RegisterValue(topic string, msg interface{}) error
}

type MessageSerializer interface {
	Serializer(schemaID int, msg interface{}) (error, []byte)
}

type MessageDeserializer interface {
	Deserialize(data []byte, dest interface{}) (int, error)
}
