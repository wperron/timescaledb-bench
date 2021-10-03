package bencher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
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
	pool    *pgxpool.Pool
	times   chan time.Duration
	stats   *Stats
	errors  chan error

	done chan bool
}

func NewBencher(ctx context.Context, w int, p *pgxpool.Pool) *Bencher {
	s := NewStats()
	m := make(map[string]*Worker)
	nodes := make([]string, 0, w)
	times := make(chan time.Duration, 100)
	errors := make(chan error)
	for i := 0; i < w; i++ {
		key := string([]byte{byte(zero + i)})
		nodes = append(nodes, key)
		worker := NewWorker(p, key, times, errors)
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
		pool:    p,
		times:   times,
		stats:   s,
		errors:  errors,
	}

	go b.recvTime()

	return b
}

func (b *Bencher) Stats() Stats {
	return *b.stats
}

func (b *Bencher) recvTime() {
	for t := range b.times {
		// fmt.Println(t)
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
			// fmt.Println(rec)
			w.in <- rec
		} else {
			fmt.Println("locking mutex")
			b.mu.Lock()
			w := NewWorker(b.pool, node, b.times, b.errors)
			b.workers[node] = w
			b.mu.Unlock()
			w.in <- rec
		}
	}

	// close(b.times)
	// close(b.errors)
}

func (b *Bencher) Wait() {
	<-b.done
}
