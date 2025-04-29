package client

import (
	"errors"
	"log/slog"
	"maps"
	"strconv"
	"strings"

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
		var b strings.Builder
		b.WriteByte('[')
		for i, c := range v {
			if i == len(v)-1 {
				b.WriteString(strconv.Itoa(int(c)))
				continue
			}
			b.WriteString(strconv.Itoa(int(c)) + ", ")
		}
		b.WriteByte(']')
		c.log.Info("Initialzing", slog.String("topic", k), slog.String("partitions", b.String()))
	}
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.log.Info("Starting to consume", slog.Int64("initial offset", claim.InitialOffset()), slog.Int("partition", int(claim.Partition())))
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return errors.New("message channel closed")
			}
			// c.log.Info("Message consumed", slog.String("topic", message.Topic),
			// slog.String("time", message.Timestamp.Format(time.RFC3339)),
			// slog.String("value", string(message.Value)))
			session.MarkMessage(message, "")
			c.s.Inc()
		case <-session.Context().Done():
			return nil
		}
	}
}
