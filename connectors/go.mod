module github.com/aaron-au/shift/connectors

go 1.26.2

require (
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.3
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aaron-au/shift/engine v0.0.0
	github.com/aaron-au/shift/sdk v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.43.0
	github.com/aws/aws-sdk-go-v2/credentials v1.19.30
	github.com/aws/aws-sdk-go-v2/service/s3 v1.106.0
	github.com/emersion/go-sasl v0.0.0-20241020182733-b788ff22d5a6
	github.com/emersion/go-smtp v0.21.3
	github.com/jackc/pgx/v5 v5.10.0
	github.com/jlaffaye/ftp v0.2.1
	github.com/pkg/sftp v1.13.11
	github.com/rabbitmq/amqp091-go v1.10.0
	golang.org/x/crypto v0.54.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.19.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.14 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.32 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.31 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.32 // indirect
	github.com/aws/smithy-go v1.27.3 // indirect
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
