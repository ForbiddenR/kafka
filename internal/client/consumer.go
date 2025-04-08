package client

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/ForbiddenR/kafka/client/internal/stats"
	"github.com/IBM/sarama"
)

type Consumer struct {
	s *stats.Stats
}

func NewConsumer(s *stats.Stats) *Consumer {
	return &Consumer{
		s,
	}
}

func (c *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	for k, v := range maps.All(s.Claims()) {
		fmt.Println("topic:", k, "partitions:", v)
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
			fmt.Printf("Message claimed: topic: %s, timestamp: %v, value: %s\n", message.Topic, message.Timestamp.Format(time.RFC3339), message.Value)
			session.MarkMessage(message, "")
			c.s.Inc()
		case <-session.Context().Done():
			return nil
		}
	}
}
