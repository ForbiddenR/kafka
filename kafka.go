package kafka

import (
	"context"
	"strings"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type KafkaWriter interface {
	Write(ctx context.Context, topic string, value []byte)
	WriteWithKey(ctx context.Context, topic, key string, value []byte)
	Start() error
	Close()
}

type kafkaWriter struct {
	addrs    []string
	username string
	password string
	logger   *zap.Logger
	client   sarama.AsyncProducer
	terminal chan struct{}
}

func NewKafkaWriter(addrs, username, password string, logger *zap.Logger) *kafkaWriter {
	return &kafkaWriter{
		addrs:    strings.Split(addrs, ","),
		username: username,
		password: password,
		logger:   logger,
		terminal: make(chan struct{}),
	}
}

func (w *kafkaWriter) Start() error {
	conf := sarama.NewConfig()
	conf.Producer.Return.Errors = false
	conf.Net.SASL.User = w.username
	conf.Net.SASL.Password = w.password
	conf.Net.SASL.Enable = true
	conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	var err error
	w.client, err = sarama.NewAsyncProducer(w.addrs, conf)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case err := <-w.client.Errors():
				w.logger.Error("kafka producer", zap.Error(err))
			case <-w.terminal:
				w.logger.Warn("kafka client closing...")
			}
		}
	}()
	return nil
}

func (w *kafkaWriter) Close() {
	if w.client != nil {
		w.client.Close()
	}
	if w.terminal != nil {
		select {
		case w.terminal <- struct{}{}:
		default:
		}
	}
}

func (w *kafkaWriter) Write(ctx context.Context, topic string, value []byte) {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	w.client.Input() <- message
}

func (w *kafkaWriter) WriteWithKey(ctx context.Context, topic, key string, value []byte) {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		message.Key = sarama.StringEncoder(topic)
	}
	w.client.Input() <- message
}
