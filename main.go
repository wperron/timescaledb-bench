package main

// Benchmark query performance on a timescale instance
// should be able to use multiple workers concurrently
// each host should always be executed by the same worker
// a single worker can handle multiple hosts

import (
	"container/heap"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
)

var (
	host = flag.String("host", "", "TimescaleDB hostname and port number")
	user = flag.String("user", "", "TimescaleDB username")
	pwd  = flag.String("password", "", "TimescaleSB password")
	db   = flag.String("db", "", "TimescaleDB database name")
	file = flag.String("file", "", "Filename containing the query parameters")
)

const (
	pattern = "2006-01-02 15:04:05"
	q       = `SELECT host, DATE_TRUNC('minute', ts), max(usage), min(usage)
	FROM cpu_usage
	WHERE host = $1
	AND ts BETWEEN $2 AND $3
	GROUP BY host, DATE_TRUNC('minute', ts)`
)

type Stats struct {
	count      int
	totalTime  time.Duration
	minTime    time.Duration
	maxTime    time.Duration
	avgTime    time.Duration
	medianTime time.Duration
	minHeap    *MinHeap
	maxHeap    *MaxHeap
}

func main() {
	flag.Parse()

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s", *user, *pwd, *host, *db)

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("connecting to TimescaleDB instance: %s", err)
	}
	defer conn.Close(ctx)

	fd, err := os.Open(*file)
	if err != nil {
		log.Fatalf("opening query params file: %s", err)
	}

	reader := csv.NewReader(fd)

	// read the header row first to move reader's cursor to first record
	head, err := reader.Read()
	if err != nil {
		log.Fatalf("reading header row from csv: %s", err)
	}

	fmt.Println(strings.Join(head, ", "))

	stats := &Stats{}
	minHeap := &MinHeap{}
	maxHeap := &MaxHeap{}
	heap.Init(minHeap)
	heap.Init(maxHeap)
	stats.minHeap = minHeap
	stats.maxHeap = maxHeap

	wg := &sync.WaitGroup{}
	wg.Add(1)
	recs := make(chan []string)
	e := make(chan error)

	go ReadRecords(reader, recs, e)
	go DoQuery(ctx, conn, stats, wg, recs, e)

	wg.Wait()

	// calculate median, average, min and max
	min, max := heap.Pop(minHeap).(time.Duration), heap.Pop(maxHeap).(time.Duration)
	stats.minTime, stats.maxTime = min, max
	for min < max {
		min, max = heap.Pop(minHeap).(time.Duration), heap.Pop(maxHeap).(time.Duration)
	}
	stats.medianTime = (min + max) / 2
	stats.avgTime = time.Duration(int(stats.totalTime) / stats.count)

	fmt.Printf("%+v\n", stats)
}

func ReadRecords(reader *csv.Reader, out chan<- []string, e chan<- error) {
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			e <- fmt.Errorf("reading record from csv: %s", err)
		}
		out <- rec
	}
	close(out)
	close(e)
}

func DoQuery(ctx context.Context, conn *pgx.Conn, stats *Stats, wg *sync.WaitGroup, recs <-chan []string, e chan<- error) {
	for rec := range recs {
		start := time.Now()
		_, err := conn.Query(ctx, q, rec[0], rec[1], rec[2])
		if err != nil {
			e <- fmt.Errorf("querying database: %s", err)
		}
		dur := time.Since(start)
		heap.Push(stats.minHeap, dur)
		heap.Push(stats.maxHeap, dur)
		stats.count += 1
		stats.totalTime += dur
	}
	wg.Done()
}
