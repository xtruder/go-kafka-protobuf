//go:generate protoc -I . --go_out=. --descriptor_set_out=./descriptor.pb --go_opt=paths=source_relative item.proto user.proto

package proto
