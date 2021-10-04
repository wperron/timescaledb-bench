package bencher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/serialx/hashring"
)

const (
	zero = 48 // byte value of '0'
)

type Bencher struct {
	ctx context.Context

	ring    *hashring.HashRing
	workers map[string]*Worker
	mu      *sync.Mutex
	times   chan time.Duration
	stats   *Stats
	errors  chan error
	cs      string // copy of the connection string

	done chan bool
}

// NewBencher creates a new instance of a Benchmark.
// ctx contains the execution context to be passed to the Bencher's various parts
// w is the number of workers to start concurrently
// cs is the connection string to use
func NewBencher(ctx context.Context, w int, cs string) (*Bencher, error) {
	s := NewStats()
	m := make(map[string]*Worker)
	nodes := make([]string, 0, w)
	times := make(chan time.Duration)
	errors := make(chan error)
	for i := 0; i < w; i++ {
		key := string([]byte{byte(zero + i)})
		nodes = append(nodes, key)
		worker, err := NewWorker(ctx, cs, key, times, errors)
		if err != nil {
			return nil, fmt.Errorf("creating worker instance: %s", err)
		}
		m[key] = worker
	}

	go func() {
		for err := range errors {
			if err != nil {
				log.Fatalln(err)
			}
		}
	}()

	b := &Bencher{
		ctx:     ctx,
		ring:    hashring.New(nodes),
		workers: m,
		mu:      &sync.Mutex{},
		times:   times,
		stats:   s,
		errors:  errors,
		cs:      cs,
		done:    make(chan bool),
	}

	go b.recvTime()

	return b, nil
}

func (b *Bencher) Stats() Stats {
	return *b.stats
}

func (b *Bencher) recvTime() {
	for t := range b.times {
		b.stats.recv(t)
	}

	b.stats.calculate()
	b.done <- true
}

func (b *Bencher) RecvRecord(recs chan []string) {
	for rec := range recs {
		node, ok := b.ring.GetNode(rec[0])
		if !ok {
			b.errors <- fmt.Errorf("no nodes in hashring")
			continue
		}

		if w, ok := b.workers[node]; ok {
			w.in <- rec
		} else {
			fmt.Println("locking mutex")
			b.mu.Lock()
			w, err := NewWorker(b.ctx, b.cs, node, b.times, b.errors)
			if err != nil {
				b.errors <- fmt.Errorf("creating worker instance on the fly: %s", err)
				continue
			}
			b.workers[node] = w
			b.mu.Unlock()
			w.in <- rec
		}
	}

	for _, w := range b.workers {
		close(w.in)
	}

	working := true
	for working {
		working = false
		for _, w := range b.workers {
			if !w.done {
				working = true
				break
			}
		}
		time.Sleep(time.Millisecond)
	}

	close(b.times)
	close(b.errors)
}

func (b *Bencher) Wait() {
	<-b.done
}
