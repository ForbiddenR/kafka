package stats

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	num       atomic.Int64
	startTime int64
}

func NewStats() *Stats {
	return &Stats{
		num:       atomic.Int64{},
		startTime: time.Now().Unix(),
	}
}

func (s *Stats) Inc() {
	s.num.Add(1)
}

func (s *Stats) Rate() int64 {
	return s.num.Load() / (time.Now().Unix() - s.startTime)
}
