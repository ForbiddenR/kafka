package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
)

type KafkaWriter struct {
	Addrs    []string
	Username string
	Password string
	client   sarama.AsyncProducer
	once     sync.Once
}

func (w *KafkaWriter) init() {
	w.once.Do(func() {
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
	})
}

func (w *KafkaWriter) Close() {
	if w.client != nil {
		w.client.Close()
	}
}

func (w *KafkaWriter) Write(ctx context.Context, topic, key string, value []byte) {
	w.init()
	w.client.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}
