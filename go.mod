module github.com/xtruder/go-kafka-protobuf

go 1.15

require (
	github.com/golang/protobuf v1.4.3
	github.com/jhump/protoreflect v1.8.1
	github.com/riferrei/srclient v0.0.0-20201205065239-9a9e8d9a1fa2
	github.com/stretchr/testify v1.3.0
	google.golang.org/protobuf v1.25.1-0.20200805231151-a709e31e5d12
)

replace github.com/riferrei/srclient => /workspace/third-party/srclient
