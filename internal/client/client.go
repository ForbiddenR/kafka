package client

import "github.com/IBM/sarama"

// var client weak.Pointer[kafka.Conn]

type KafkaClient struct {
	Brokers      string
	Topic        string
	PartitionNum int
	Username     string
	Password     string
}

func (c *KafkaClient) init() {
	conf := sarama.NewConfig()
	conf.ClientID = "test"
	admin, err := sarama.NewClusterAdmin([]string{}, conf)
	if err != nil {
		panic(err)
	}
	admin.ListTopics()
	// conn := client.Value()
	// if conn == nil {
	// 	mechanism := plain.Mechanism{
	// 		Username: c.Username,
	// 		Password: c.Password,
	// 	}
	// 	dialer := kafka.Dialer{
	// 		Timeout:       5 * time.Second,
	// 		DualStack:     true,
	// 		SASLMechanism: mechanism,
	// 	}
	// 	conn, err := dialer.Dial("tcp", c.Brokers)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	client = weak.Make(conn)
	// 	runtime.AddCleanup(conn, func(conn *kafka.Conn) { conn.Close() }, conn)
	// 	return conn
	// }
	// return conn
}

func (c *KafkaClient) List() error {
	// partitions, err := c.init().ReadPartitions()
	// if err != nil {
	// 	return err
	// }
	// for _, partition := range partitions {
	// 	fmt.Println(partition.Topic, partition.ID, fmt.Sprintf("leader id: %d, address: %s:%d", partition.Leader.ID, partition.Leader.Host, partition.Leader.Port))
	// }
	return nil
}

func (c *KafkaClient) CreateTopic() error {
	// return c.init().CreateTopics(kafka.TopicConfig{
	// 	Topic:         c.Topic,
	// 	NumPartitions: c.PartitionNum,
	// })
	return nil
}

func (c *KafkaClient) DeleteTopic() error {
	// return c.init().DeleteTopics(c.Topic)
	return nil
}
