package statistics

import (
	"bufio"
	"encoding/json"
	"io"
	"math/rand/v2"
	"os"
	"time"
)

var (
	protocols, commands []string
)

func init() {
	var err error
	protocols, err = readFileByLine("protocol.line")
	if err != nil {
		panic(err)
	}
	commands, err = readFileByLine("command.line")
	if err != nil {
		panic(err)
	}
}

func readFileByLine(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	buffer := bufio.NewReader(file)
	lines := make([]string, 0)
	for {
		line, _, err := buffer.ReadLine()
		if err != nil {
			if err == io.EOF {
				return lines, nil
			}
			return nil, err
		}
		lines = append(lines, string(line))
	}
}

type Statistics struct {
	Timestamp int64  `json:"timestamp"`
	Protocol  string `json:"protocol"`
	Command   string `json:"schema"`
	Hostname  string `json:"host"`
}

func NewStatisticsData(hostname string) *Statistics {
	return &Statistics{
		Timestamp: time.Now().Unix(),
		Protocol:  protocols[int(rand.IntN(len(protocols)))],
		Command:   commands[int(rand.IntN(len(commands)))],
		Hostname:  hostname,
	}
}

func (s *Statistics) ToJson() ([]byte, error) {
	return json.Marshal(s)
}
