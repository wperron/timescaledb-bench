package bencher

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
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
	conn   *pgx.Conn
}

func NewWorker(ctx context.Context, cs string, id string, out chan time.Duration, errors chan error) (*Worker, error) {
	in := make(chan []string)

	conn, err := pgx.Connect(ctx, cs)
	if err != nil {
		return nil, fmt.Errorf("connecting to timescale instance: %s", err)
	}

	w := &Worker{
		id:     id,
		in:     in,
		out:    out,
		errors: errors,
		conn:   conn,
	}

	go w.do(context.Background(), conn)

	return w, nil
}

func (w *Worker) do(ctx context.Context, conn *pgx.Conn) {
	for rec := range w.in {
		timed, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		start := time.Now()
		_, err := conn.Query(timed, q, rec[0], rec[1], rec[2])
		if err != nil {
			w.errors <- fmt.Errorf("querying database: %s", err)
			continue
		}
		dur := time.Since(start)
		w.out <- dur
	}
}
