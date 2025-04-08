package client

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

type Consumer struct {
}

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
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
			fmt.Printf("Message claimed: topic: %s, timestamp: %v, value: %s", message.Topic, message.Timestamp, message.Value)
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
