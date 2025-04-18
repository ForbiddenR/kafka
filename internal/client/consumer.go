package client

import (
	"errors"
	"log/slog"
	"maps"
	"time"

	"github.com/ForbiddenR/kafka/client/internal/stats"
	"github.com/IBM/sarama"
)

type Consumer struct {
	s   *stats.Stats
	log *slog.Logger
}

func NewConsumer(s *stats.Stats, log *slog.Logger) *Consumer {
	return &Consumer{
		s,
		log,
	}
}

func (c *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	for k, v := range maps.All(s.Claims()) {
		c.log.Info("Initialzing", slog.String("topic", k), slog.Any("partitions", v))
	}
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return errors.New("message channel closed")
			}
			// fmt.Printf("Message claimed: topic: %s, timestamp: %v, value: %s\n", message.Topic, message.Timestamp.Format(time.RFC3339), message.Value)
			c.log.Info("Message consumed", slog.String("topic", message.Topic), 
			slog.String("time", message.Timestamp.Format(time.RFC3339)),
			slog.String("value", string(message.Value)))
			session.MarkMessage(message, "")
			c.s.Inc()
		case <-session.Context().Done():
			return nil
		}
	}
}
