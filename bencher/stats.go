package bencher

import (
	"container/heap"
	"time"
)

type Stats struct {
	Count      int
	TotalTime  time.Duration
	MinTime    time.Duration
	MaxTime    time.Duration
	AvgTime    time.Duration
	MedianTime time.Duration
	minHeap    *minHeap
	maxHeap    *maxHeap
}

func NewStats() *Stats {
	minHeap := &minHeap{}
	maxHeap := &maxHeap{}
	heap.Init(minHeap)
	heap.Init(maxHeap)

	return &Stats{
		minHeap: minHeap,
		maxHeap: maxHeap,
	}
}

func (s *Stats) recv(d time.Duration) {
	heap.Push(s.minHeap, d)
	heap.Push(s.maxHeap, d)
	s.Count += 1
	s.TotalTime += d
}

func (s *Stats) calculate() {
	// calculate median, average, min and max
	min, max := heap.Pop(s.minHeap).(time.Duration), heap.Pop(s.maxHeap).(time.Duration)
	s.MinTime, s.MaxTime = min, max
	for min < max {
		min, max = heap.Pop(s.minHeap).(time.Duration), heap.Pop(s.maxHeap).(time.Duration)
	}
	s.MedianTime = (min + max) / 2
	s.AvgTime = time.Duration(int(s.TotalTime) / s.Count)
}
