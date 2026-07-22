module github.com/aaron-au/shift/connectors

go 1.26.2

require (
	github.com/aaron-au/shift/engine v0.0.0
	github.com/aaron-au/shift/sdk v0.0.0
	github.com/pkg/sftp v1.13.11
	golang.org/x/crypto v0.54.0
)

require github.com/kr/fs v0.1.0 // indirect

require (
	github.com/aaron-au/shift/pkg v0.0.0
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/aaron-au/shift/engine => ../engine

replace github.com/aaron-au/shift/sdk => ../sdk

replace github.com/aaron-au/shift/pkg => ../pkg
