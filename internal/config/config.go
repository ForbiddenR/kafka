package config

import (
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

var config Config
var once sync.Once

type Config struct {
	Brokers  string `yaml:"brokers"`
	Topic    string `yaml:"topic"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func NewConfig(path string) Config {
	once.Do(func() {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		config = Config{}
		err = yaml.NewDecoder(file).Decode(&config)
		if err != nil {
			panic(err)
		}
	})
	return config
}
