package redisconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisClient is the minimal set of Redis operations the verbs use. Defining it
// here (rather than depending on *redis.Client directly) lets every verb be
// unit-tested against an in-memory fake with no real broker — go-redis is
// confined to this file's adapter (see redisconn_test.go).
type redisClient interface {
	// Scan performs one SCAN step: returns a page of keys matching match and
	// the next cursor (0 means iteration is complete).
	Scan(ctx context.Context, cursor uint64, match string, count int64) (keys []string, next uint64, err error)
	Type(ctx context.Context, key string) (string, error)
	Get(ctx context.Context, key string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) (deleted int64, err error)
	Close() error
}

// openClient is the production redisClient factory (injected into every verb by
// Connector; tests substitute a fake). The connection is network-guarded and,
// when configured, TLS-wrapped; go-redis dials lazily on first command.
//
// The TLS wrap happens HERE, inside the dial function, rather than by setting
// opts.TLSConfig — and that is not a style choice. go-redis honors TLSConfig
// only from its own default dialer: options.go does
// `if opt.Dialer == nil { opt.Dialer = NewDialer(opt) }`, and the
// tls.DialWithDialer call lives solely inside NewDialer. Supplying a custom
// Dialer (which the network guard requires) therefore makes TLSConfig dead
// config — the AUTH password and every value would cross the wire in cleartext
// while the flow author believes TLS is on, with no error to notice.
func openClient(cfg *config) (redisClient, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, Control: guard(cfg.AllowLocal)}
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil || !cfg.TLS {
			return conn, err
		}
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			host = addr
		}
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("redis: TLS handshake with %s: %w", addr, err)
		}
		return tlsConn, nil
	}
	return &goRedisClient{c: redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
		Dialer:   dial,
	})}, nil
}

// goRedisClient adapts *redis.Client to redisClient. Each method is a thin
// passthrough converting go-redis's typed command results to plain Go values.
type goRedisClient struct{ c *redis.Client }

func (g *goRedisClient) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return g.c.Scan(ctx, cursor, match, count).Result()
}

func (g *goRedisClient) Type(ctx context.Context, key string) (string, error) {
	return g.c.Type(ctx, key).Result()
}

func (g *goRedisClient) Get(ctx context.Context, key string) (string, error) {
	v, err := g.c.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) { // key vanished between SCAN and GET — treat as empty
		return "", nil
	}
	return v, err
}

func (g *goRedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return g.c.HGetAll(ctx, key).Result()
}

func (g *goRedisClient) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return g.c.LRange(ctx, key, start, stop).Result()
}

func (g *goRedisClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return g.c.Set(ctx, key, value, ttl).Err()
}

func (g *goRedisClient) Del(ctx context.Context, keys ...string) (int64, error) {
	return g.c.Del(ctx, keys...).Result()
}

func (g *goRedisClient) Close() error { return g.c.Close() }
