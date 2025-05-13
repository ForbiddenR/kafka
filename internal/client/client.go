package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	mrand "math/rand/v2"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ForbiddenR/kafka/client/internal/stats"
	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
)

type Build interface {
	ConfigToKafkaClient(*KafkaClient)
}

type KafkaClient struct {
	Brokers      string
	Topic        string
	PartitionNum int
	Username     string
	Password     string
	clientId     string
}

func NewKafkaClient(b Build) *KafkaClient {
	kc := &KafkaClient{}
	b.ConfigToKafkaClient(kc)
	return kc
}

func (c *KafkaClient) addSASL(cfg *sarama.Config) {
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.User = c.Username
	cfg.Net.SASL.Password = c.Password
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
}

func (c *KafkaClient) produce() (sarama.AsyncProducer, error) {
	cfg := sarama.NewConfig()
	suffix, err := getSuffix()
	if err != nil {
		return nil, err
	}
	c.clientId = "producer" + suffix
	cfg.ClientID = c.clientId
	cfg.Version = sarama.V4_0_0_0
	if c.Username != "" && c.Password != "" {
		c.addSASL(cfg)
	}
	return sarama.NewAsyncProducer(strings.Split(c.Brokers, ","), cfg)
}

func (c *KafkaClient) fetch() (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	suffix, err := getSuffix()
	if err != nil {
		return nil, err
	}
	c.clientId = "fetch" + suffix
	cfg.ClientID = c.clientId
	cfg.Version = sarama.V4_0_0_0
	if c.Username != "" && c.Password != "" {
		c.addSASL(cfg)
	}
	return sarama.NewConsumer(strings.Split(c.Brokers, ","), cfg)
}

func (c *KafkaClient) consume(groupId string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	suffix, err := getSuffix()
	if err != nil {
		return nil, err
	}
	c.clientId = "consumer" + suffix
	cfg.ClientID = c.clientId
	cfg.Version = sarama.V4_0_0_0
	cfg.Consumer.Offsets.AutoCommit.Interval = time.Second * 3
	if c.Username != "" && c.Password != "" {
		c.addSASL(cfg)
	}
	return sarama.NewConsumerGroup(strings.Split(c.Brokers, ","), groupId, cfg)
}

func (c *KafkaClient) admin() (sarama.ClusterAdmin, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = "admin-client"
	cfg.Version = sarama.V4_0_0_0
	if c.Username != "" && c.Password != "" {
		c.addSASL(cfg)
	}
	return sarama.NewClusterAdmin(strings.Split(c.Brokers, ","), cfg)
}

func (c *KafkaClient) Produce(prefix, topic string) error {
	producer, err := c.produce()
	if err != nil {
		return err
	}
	closeChan := make(chan struct{})
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(3)

	s := stats.NewStats()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("client", c.clientId)

	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Minute)
		for {
			select {
			case <-closeChan:
				return
			case <-t.C:
				log.Info(fmt.Sprintf("produce rate %g r/s", s.Rate()))
			}
		}
	}()

	for id := range 50000 {
		wg.Add(1)
		go func() {
			time.Sleep(time.Second * time.Duration(mrand.IntN(20)))
			i := 0
			defer wg.Done()
			for {
				select {
				case <-closeChan:
					return
				default:
				}
				time.Sleep(time.Second * 10)
				producer.Input() <- &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder(fmt.Sprintf("%s-%d message %d", prefix, id, i)),
				}
				s.Inc()
				i++
			}
		}()
	}

	go func() {
		defer wg.Done()
		for {
			select {
			case <-closeChan:
				return
			case msg := <-producer.Errors():
				log.Error("produce error", slog.Any("err", msg.Err))
			}
		}
	}()

	go func() {
		defer wg.Done()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, os.Interrupt, syscall.SIGTERM)
		<-sig
		close(closeChan)
	}()
	wg.Wait()
	return nil
}

func (c *KafkaClient) Fetch(key, topic string, dur time.Duration) error {
	client, err := c.fetch()
	if err != nil {
		return err
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, cx := errgroup.WithContext(ctx)
	for partition := range slices.Values(partitions) {
		eg.Go(func() error {
			partition, err := client.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err != nil {
				return err
			}
			defer partition.Close()
			for {
				select {
				case <-cx.Done():
					return cx.Err()
				case msg := <-partition.Messages():
					if string(msg.Key) == key && time.Since(msg.Timestamp) < dur {
						fmt.Printf("%s %s. %s\n", msg.Timestamp.Format(time.RFC3339), string(msg.Value), string(msg.Key))
					}
				}
			}
		})
	}
	return eg.Wait()
}

func (c *KafkaClient) Consume(groupId string, topics ...string) error {
	client, err := c.consume(groupId)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := stats.NewStats()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With(slog.String("client", c.clientId))

	go func() {
		t := time.NewTicker(time.Minute)
		for range t.C {
			log.Info(fmt.Sprintf("consume rate %g r/s", s.Rate()))
		}
	}()

	for {
		if err := client.Consume(ctx, topics, NewConsumer(s, log)); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return err
			}
		}
		log.Error("general error when consuming", slog.Any("err", err))
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (c *KafkaClient) List() error {
	client, err := c.admin()
	if err != nil {
		return err
	}
	topics, err := client.ListTopics()
	if err != nil {
		return err
	}
	for topic, detail := range maps.All(topics) {
		if !strings.HasPrefix(topic, "__") {
			fmt.Printf("topic: %s - partition number: %d\n", topic, detail.NumPartitions)
		}
	}
	return nil
}

func (c *KafkaClient) Description(topic string) error {
	client, err := c.admin()
	if err != nil {
		return err
	}
	groups, err := client.ListConsumerGroups()
	if err != nil {
		return err
	}
	for group, state := range maps.All(groups) {
		fmt.Printf("group: %s - state: %s\n", group, state)
	}
	return nil
}

func (c *KafkaClient) CreateTopic(topic string, partition int32) error {
	if topic == "" {
		return errors.New("topic is empty")
	}
	client, err := c.admin()
	if err != nil {
		return err
	}
	return client.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partition),
		ReplicationFactor: 1,
	}, false)
}

func (c *KafkaClient) DeleteTopic(topic string) error {
	if topic == "" {
		return errors.New("topic is empty")
	}
	client, err := c.admin()
	if err != nil {
		return err
	}
	return client.DeleteTopic(topic)
}

func getSuffix() (string, error) {
	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%X", suffix), nil
}
