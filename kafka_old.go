//go:build go1.22 && !go1.23

package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type KakfaWriter struct {
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
		writer: &kafka.Writer{
			Addr:         kafka.TCP(borkers),
			Transport:    transport,
			RequiredAcks: 0,
		},
	}
}

func (k *KakfaWriter) Wrtie(ctx context.Context, topic, key string, value []byte) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
}
