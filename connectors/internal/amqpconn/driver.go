package amqpconn

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// heartbeat is the AMQP connection heartbeat interval requested of the broker.
const heartbeat = 10 * time.Second

// dialConn opens a network-guarded AMQP connection and one channel. It is the
// shared front half of both real dialers; the caller decides what to declare
// on the channel.
func dialConn(cfg *Config) (*amqp.Connection, *amqp.Channel, error) {
	dialURL := cfg.amqpURL()
	acfg := amqp.Config{
		Heartbeat: heartbeat,
		Dial:      cfg.guardedDial,
	}
	if strings.HasPrefix(dialURL, "amqps") {
		acfg.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: cfg.serverName(),
		}
	}
	conn, err := amqp.DialConfig(dialURL, acfg)
	if err != nil {
		return nil, nil, fmt.Errorf("amqp: dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("amqp: open channel: %w", err)
	}
	return conn, ch, nil
}

// realConn adapts an amqp091 connection+channel to the publishChannel and
// consumeChannel seams.
type realConn struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

func (c *realConn) Close() error {
	// Closing the connection tears the channel down with it.
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *realConn) PublishBody(ctx context.Context, exchange, key, contentType, messageID string, body []byte) error {
	return c.ch.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{
		ContentType:  contentType,
		MessageId:    messageID,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	})
}

func (c *realConn) GetNext(_ context.Context) (delivery, bool, error) {
	msg, ok, err := c.ch.Get(c.queue, false) // autoAck=false: we ack explicitly
	if err != nil || !ok {
		return delivery{}, ok, err
	}
	m := msg // capture for the ack closure
	return delivery{
		Body:        m.Body,
		RoutingKey:  m.RoutingKey,
		Exchange:    m.Exchange,
		Headers:     map[string]any(m.Headers),
		DeliveryTag: m.DeliveryTag,
		Ack:         func() error { return m.Ack(false) },
	}, true, nil
}

// dialPublish is the production publishDialer.
func dialPublish(_ context.Context, cfg *Config) (publishChannel, error) {
	conn, ch, err := dialConn(cfg)
	if err != nil {
		return nil, err
	}
	return &realConn{conn: conn, ch: ch}, nil
}

// dialConsume is the production consumeDialer: it declares the queue (durable
// per config) and sets the channel prefetch before returning.
func dialConsume(_ context.Context, cfg *Config) (consumeChannel, error) {
	conn, ch, err := dialConn(cfg)
	if err != nil {
		return nil, err
	}
	if _, err := ch.QueueDeclare(cfg.Queue, cfg.Durable, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("amqp: declare queue %q: %w", cfg.Queue, err)
	}
	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("amqp: set qos: %w", err)
	}
	return &realConn{conn: conn, ch: ch, queue: cfg.Queue}, nil
}
