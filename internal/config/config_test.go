package config

import (
	"fmt"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfig(t *testing.T) {
	data := `brokers: fdfgfgfg`
	result := &Config{}
	err := yaml.Unmarshal([]byte(data), result)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
