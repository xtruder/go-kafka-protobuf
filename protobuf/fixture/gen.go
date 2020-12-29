//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative item.proto user.proto

package fixture
