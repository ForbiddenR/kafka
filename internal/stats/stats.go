package stats

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	num       atomic.Int64
	lastCount int64
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

func (s *Stats) Rate() float64 {
	count := s.num.Load()
	defer func() {
		s.lastCount = count
		s.startTime = time.Now().Unix()
	}()
	return float64(count-s.lastCount) / float64((time.Now().Unix() - s.startTime))
}
