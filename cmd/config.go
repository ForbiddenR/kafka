package cmd

import (
	"context"

	"github.com/ForbiddenR/kafka/client/internal/client"
)

type cfgKey struct{}

type Config struct {
	Brokers  string  `yaml:"brokers"`
	Username *string `yaml:"username"`
	Password *string `yaml:"password"`
}

func (c *Config) ConfigToKafkaClient(kc *client.KafkaClient) {
	kc.Brokers = c.Brokers
	if c.Username != nil {
		kc.Username = *c.Username
	}
	if c.Password != nil {
		kc.Password = *c.Password
	}
}

func putConfig(ctx context.Context, cfg *Config) context.Context {
	return context.WithValue(ctx, cfgKey{}, cfg)
}

func getConfig(ctx context.Context) *Config {
	return ctx.Value(cfgKey{}).(*Config)
}
