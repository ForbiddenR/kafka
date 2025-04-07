package client

import (
	"errors"
	"fmt"
	"maps"
	"strings"

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

func (c KafkaClient) admin() (sarama.ClusterAdmin, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = "admin-client"
	cfg.Version = sarama.V4_0_0_0
	return sarama.NewClusterAdmin(strings.Split(c.Brokers, ","), cfg)
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
