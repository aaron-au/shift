package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/shift/runner/internal/logger"
)

// Consumer handles Kafka message consumption
type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	logger        *logger.Logger
	onMessage     func(topic string, key string, value []byte)
	mu            sync.RWMutex
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(brokers []string, groupID string, log *logger.Logger) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_7_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return &Consumer{
		consumerGroup: consumerGroup,
		logger:        log,
	}, nil
}

// SetMessageHandler sets the callback for when messages are received
func (c *Consumer) SetMessageHandler(handler func(topic string, key string, value []byte)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onMessage = handler
}

// Subscribe subscribes to Kafka topics
func (c *Consumer) Subscribe(ctx context.Context, topics []string) error {
	handler := &consumerGroupHandler{
		consumer: c,
		logger:   c.logger,
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := c.consumerGroup.Consume(ctx, topics, handler)
			if err != nil {
				c.logger.Error("Error from consumer: %v", err)
				return err
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return c.consumerGroup.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	consumer *Consumer
	logger   *logger.Logger
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message := <-claim.Messages():
			if message == nil {
				continue
			}

			h.consumer.mu.RLock()
			handler := h.consumer.onMessage
			h.consumer.mu.RUnlock()

			if handler != nil {
				handler(message.Topic, string(message.Key), message.Value)
			}

			session.MarkMessage(message, "")
		}
	}
}

// Producer handles Kafka message production
type Producer struct {
	producer sarama.SyncProducer
	logger   *logger.Logger
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string, log *logger.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_7_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer: producer,
		logger:   log,
	}, nil
}

// Publish publishes a message to a Kafka topic
func (p *Producer) Publish(topic string, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	p.logger.Info("Published message to topic %s, partition %d, offset %d", topic, partition, offset)
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.producer.Close()
}

