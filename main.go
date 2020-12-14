package main

import (
	"fmt"
	"os"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	prot "github.com/xtruder/go-kafka-protobuf/proto"
)

var printer = protoprint.Printer{
	ForceFullyQualifiedNames: true,
}

func printFilesRecursive(file *desc.FileDescriptor) {
	deps := file.GetDependencies()

	for _, dep := range deps {
		printFilesRecursive(dep)
		fmt.Println(printer.PrintProtoToString(dep))
		//printer.PrintProtoFile(dep, os.Stdout)
	}
}

func main() {
	msg := &prot.User{Id: "test", Message: "test"}

	desc, err := desc.LoadMessageDescriptorForMessage(msg)
	if err != nil {
		panic(err)
	}

	printer.PrintProtoFile(desc.GetFile(), os.Stdout)

	desc.GetFile().GetMessageTypes()[0].GetNestedMessageTypes()

	printFilesRecursive(desc.GetFile())
	// println(msgDescriptor.FullName())

	// println(msgDescriptor.ParentFile().Imports().Get(0).Path())

	// print(msgDescriptor.ParentFile().FullName())

	// for i := 0; i < msgDescriptor.Fields().Len(); i++ {
	// 	fieldDescriptor := msgDescriptor.Fields().Get(i)
	// 	println(fieldDescriptor.FullName())

	// 	if fieldDescriptor.Message() != nil {
	// 		subMsg := fieldDescriptor.Message()
	// 		println(subMsg.FullName())
	// 		println(subMsg.ParentFile().FullName())
	// 	}
	// }
}
