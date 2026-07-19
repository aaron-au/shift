module github.com/aaron-au/shift/hub

go 1.26.2

require (
	github.com/aaron-au/shift/pkg v0.0.0-00010101000000-000000000000
	github.com/coreos/go-oidc/v3 v3.20.0
	github.com/go-jose/go-jose/v4 v4.1.4
	github.com/jackc/pgx/v5 v5.10.0
)

require (
	github.com/aaron-au/shift/engine v0.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.36.0 // indirect
)

replace github.com/aaron-au/shift/pkg => ../pkg

replace github.com/aaron-au/shift/engine => ../engine
