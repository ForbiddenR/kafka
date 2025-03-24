package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type KafkaWriter struct {
	Addrs    []string
	Username string
	Password string
	Logger   *zap.Logger
	client   sarama.AsyncProducer
	once     sync.Once
	terminal chan struct{}
}

func (w *KafkaWriter) init() {
	w.once.Do(func() {
		w.terminal = make(chan struct{})
		conf := sarama.NewConfig()
		conf.Producer.Return.Errors = false
		conf.Net.SASL.User = w.Username
		conf.Net.SASL.Password = w.Password
		conf.Net.SASL.Enable = true
		conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		var err error
		w.client, err = sarama.NewAsyncProducer(w.Addrs, conf)
		if err != nil {
			panic(err)
		}
		go func() {
			for {
				select {
				case err := <-w.client.Errors():
					w.Logger.Error("kafka producer error", zap.Error(err))
				case <-w.terminal:
					return
				}
			}
		}()
	})
}

func (w *KafkaWriter) Close() {
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

func (w *KafkaWriter) Write(ctx context.Context, topic, key string, value []byte) {
	w.init()
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key == "" {
		message.Key = sarama.StringEncoder(topic)
	}
	w.client.Input() <- message
}
