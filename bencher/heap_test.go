package bencher

import (
	"container/heap"
	"testing"
	"time"
)

func TestMinHeap(t *testing.T) {
	h := &minHeap{}
	heap.Init(h)

	heap.Push(h, 1*time.Millisecond)
	heap.Push(h, 3*time.Millisecond)
	heap.Push(h, 5*time.Millisecond)
	heap.Push(h, 2*time.Millisecond)

	next := heap.Pop(h).(time.Duration)
	min := next
	for h.Len() > 0 {
		next := heap.Pop(h).(time.Duration)
		if next < min {
			t.Errorf("expected %d to be smaller than %d", min, next)
		}
	}
}

func TestMaxHeap(t *testing.T) {
	h := &maxHeap{}
	heap.Init(h)

	heap.Push(h, 1*time.Millisecond)
	heap.Push(h, 3*time.Millisecond)
	heap.Push(h, 5*time.Millisecond)
	heap.Push(h, 2*time.Millisecond)

	next := heap.Pop(h).(time.Duration)
	max := next
	for h.Len() > 0 {
		next := heap.Pop(h).(time.Duration)
		if next > max {
			t.Errorf("expected %d to be bigger than %d", max, next)
		}
	}
}
