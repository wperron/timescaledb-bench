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

type Report struct {
	count      int
	totalTime  time.Duration
	minTime    time.Duration
	maxTime    time.Duration
	avgTime    time.Duration
	medianTime time.Duration
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

	report := Report{}
	minHeap := &MinHeap{}
	maxHeap := &MaxHeap{}
	heap.Init(minHeap)
	heap.Init(maxHeap)

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("reading record from csv: %s", err)
		}

		start := time.Now()
		_, err = conn.Query(ctx, q, rec[0], rec[1], rec[2])
		if err != nil {
			log.Fatalf("querying database: %s", err)
		}
		dur := time.Since(start)
		heap.Push(minHeap, dur)
		heap.Push(maxHeap, dur)

		report.count += 1
		report.totalTime += dur
		report.avgTime = time.Duration(int(report.totalTime) / report.count)
		report.minTime = minDuration(report.minTime, dur)
		report.maxTime = maxDuration(report.maxTime, dur)
	}

	// calculate median
	min, max := minHeap.Pop().(time.Duration), maxHeap.Pop().(time.Duration)
	for min < max {
		min, max = minHeap.Pop().(time.Duration), maxHeap.Pop().(time.Duration)
	}
	report.medianTime = (min + max) / 2

	fmt.Printf("%+v\n", report)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
