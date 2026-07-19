module github.com/aaron-au/shift/runner

go 1.26.2

require (
	github.com/aaron-au/shift/engine v0.0.0
	github.com/aaron-au/shift/sdk v0.0.0
)

require (
	github.com/aaron-au/shift/pkg v0.0.0
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/aaron-au/shift/engine => ../engine

replace github.com/aaron-au/shift/sdk => ../sdk

replace github.com/aaron-au/shift/pkg => ../pkg
