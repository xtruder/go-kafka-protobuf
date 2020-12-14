package schema

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"github.com/riferrei/srclient"
)

type ProtobufSchemaRegistrator struct {
	registryClient srclient.ISchemaRegistryClient
	printer        *protoprint.Printer
}

func NewProtobufSchemaRegistrator(regClient srclient.ISchemaRegistryClient) *ProtobufSchemaRegistrator {
	printer := &protoprint.Printer{ForceFullyQualifiedNames: true}

	return &ProtobufSchemaRegistrator{
		registryClient: regClient,
		printer:        printer,
	}
}

func (r *ProtobufSchemaRegistrator) Register(topic string, record interface{}) error {
	msg, ok := record.(proto.Message)
	if !ok {
		return fmt.Errorf("record type must be of proto.Message")
	}

	msgDesc, err := desc.LoadMessageDescriptorForMessage(msg)
	if err != nil {
		return fmt.Errorf("error loading message desciprot for message %w", err)
	}

	fileDesc := msgDesc.GetFile()
	deps := collectFileDescDeps(fileDesc)

	refs := []srclient.Reference{}
	for _, dep := range deps {
		depSchema, err := fileDescriptorToSchemaString(r.printer, dep)
		if err != nil {
			return err
		}

		name := dep.GetName()
		schema, err := r.registryClient.CreateSchema(name, depSchema, srclient.Protobuf)
		if err != nil {
			return fmt.Errorf("Error creating schema: %w", err)
		}

		refs = append(refs, srclient.Reference{
			Name:    name,
			Subject: name,
			Version: schema.Version(),
		})
	}

	schema, err := fileDescriptorToSchemaString(r.printer, fileDesc)
	if err != nil {
		return err
	}

	_, err = r.registryClient.CreateSchema(topic, schema, srclient.Protobuf, refs...)
	if err != nil {
		return fmt.Errorf("Error creating schema: %w", err)
	}

	return nil
}

func fileDescriptorToSchemaString(printer *protoprint.Printer, file *desc.FileDescriptor) (string, error) {
	result, err := printer.PrintProtoToString(file)
	if err != nil {
		return "", fmt.Errorf("Error converting proto file descriptor to schema string: %w", err)
	}

	return result, nil
}

// reursively collect file descriptor dependencies
func collectFileDescDeps(file *desc.FileDescriptor) []*desc.FileDescriptor {
	var collectDeps func(file *desc.FileDescriptor) []*desc.FileDescriptor
	collectDeps = func(file *desc.FileDescriptor) (deps []*desc.FileDescriptor) {
		for _, dep := range file.GetDependencies() {
			deps = append(deps, dep)
			deps = append(deps, collectDeps(dep)...)
		}

		return
	}

	return collectDeps(file)
}
