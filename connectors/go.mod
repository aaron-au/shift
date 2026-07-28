module github.com/aaron-au/shift/connectors

go 1.26.2

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aaron-au/shift/engine v0.0.0
	github.com/aaron-au/shift/sdk v0.0.0
	github.com/emersion/go-sasl v0.0.0-20241020182733-b788ff22d5a6
	github.com/emersion/go-smtp v0.21.3
	github.com/jackc/pgx/v5 v5.10.0
	github.com/pkg/sftp v1.13.11
	golang.org/x/crypto v0.54.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/fs v0.1.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
)

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
