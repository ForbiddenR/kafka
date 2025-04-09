package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ForbiddenR/kafka/client/internal/stats"
	"github.com/IBM/sarama"
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
	cfg.ClientID = "producer-client"
	cfg.Version = sarama.V4_0_0_0
	if c.Username != "" && c.Password != "" {
		c.addSASL(cfg)
	}
	return sarama.NewAsyncProducer(strings.Split(c.Brokers, ","), cfg)
}

func (c *KafkaClient) consume(groupId string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = "consumer-client"
	cfg.Version = sarama.V4_0_0_0
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
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-closeChan:
				return
			case <-t.C:
				fmt.Println("produce rate:", s.Rate())
			}
		}
	}()

	for id := range 20000 {
		wg.Add(1)
		go func() {
			i := 0
			defer wg.Done()
			for {
				select {
				case <-closeChan:
					return
				default:
				}
				time.Sleep(time.Second * 2)
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
				fmt.Println("producer error", msg.Err)
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

func (c *KafkaClient) Consume(groupId string, topics ...string) error {
	client, err := c.consume(groupId)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := stats.NewStats()

	go func() {
		t := time.NewTicker(10 * time.Second)
		for range t.C {
			fmt.Println("consume rate:", s.Rate())
		}
	}()

	for {
		if err := client.Consume(ctx, topics, NewConsumer(s)); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return err
			}
		}
		fmt.Println("general error when consuming", err)
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
