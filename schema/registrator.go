package schema

type SchemaRegistrator interface {
	Register(topic string, record interface{}) error
}
