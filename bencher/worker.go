package bencher

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	q = `SELECT host, DATE_TRUNC('minute', ts), max(usage), min(usage)
	FROM cpu_usage
	WHERE host = $1
	AND ts BETWEEN $2 AND $3
	GROUP BY host, DATE_TRUNC('minute', ts)`
)

type Worker struct {
	id     string
	in     chan []string
	out    chan time.Duration
	errors chan error
}

func NewWorker(p *pgxpool.Pool, id string, out chan time.Duration, errors chan error) *Worker {
	in := make(chan []string)

	w := &Worker{
		id:     id,
		in:     in,
		out:    out,
		errors: errors,
	}

	go w.do(context.Background(), p)

	return w
}

func (w *Worker) do(ctx context.Context, p *pgxpool.Pool) {
	for rec := range w.in {
		timed, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		start := time.Now()
		fmt.Printf("[%s] starting query -> %v\n", w.id, rec)
		_, err := p.Query(timed, q, rec[0], rec[1], rec[2])
		fmt.Printf("[%s] query done\n", w.id)
		if err != nil {
			w.errors <- fmt.Errorf("querying database: %s", err)
			continue
		}
		dur := time.Since(start)
		w.out <- dur
	}
}
