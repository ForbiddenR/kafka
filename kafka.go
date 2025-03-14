//go:build go1.23

package kafka

import (
	"context"
	"time"
	"unique"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type KakfaWriter struct {
	topic  unique.Handle[string]
	writer *kafka.Writer
}

func NewKafkaWriter(borkers, topic, username, password string, timeout time.Duration) *KakfaWriter {
	mechanism := &plain.Mechanism{
		Username: username,
		Password: password,
	}
	transport := &kafka.Transport{
		DialTimeout: timeout,
		IdleTimeout: timeout,
		SASL:        mechanism,
	}
	return &KakfaWriter{
		topic: unique.Make(topic),
		writer: &kafka.Writer{
			Addr:         kafka.TCP(borkers),
			Topic:        topic,
			Transport:    transport,
			RequiredAcks: 0,
		},
	}
}

func (k *KakfaWriter) Wrtie(ctx context.Context, _ string, value []byte) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Topic: k.topic.Value(),
		Value: value,
	})
}
