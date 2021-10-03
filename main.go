package main

// Benchmark query performance on a timescale instance
// should be able to use multiple workers concurrently
// each host should always be executed by the same worker
// a single worker can handle multiple hosts

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/wperron/timescaledb-bench/bencher"
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
	workers = 5
)

func main() {
	flag.Parse()

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s", *user, *pwd, *host, *db)
	ctx := context.Background()
	timeout, _ := context.WithTimeout(ctx, 5*time.Second)

	pool, err := pgxpool.Connect(timeout, connStr)
	if err != nil {
		log.Fatalf("connecting to TimescaleDB instance: %s", err)
	}
	defer pool.Close()

	fd, err := os.Open(*file)
	if err != nil {
		log.Fatalf("opening query params file: %s", err)
	}

	reader := csv.NewReader(fd)

	// read the header row first to move reader's cursor to first record
	_, err = reader.Read()
	if err != nil {
		log.Fatalf("reading header row from csv: %s", err)
	}

	bencher := bencher.NewBencher(ctx, workers, pool)
	recs := make(chan []string, 100)
	e := make(chan error, 100)

	go func() {
		for err := range e {
			if err != nil {
				log.Fatalln(err)
			}
		}
	}()

	go ReadRecords(reader, recs, e)
	go bencher.RecvRecord(recs)

	bencher.Wait()

	fmt.Printf("%+v\n", bencher.Stats())
}

func ReadRecords(reader *csv.Reader, out chan<- []string, e chan<- error) {
	count := 0
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			fmt.Println("EOF reached")
			break
		}
		if err != nil {
			e <- fmt.Errorf("reading record from csv: %s", err)
		}
		out <- rec
		count++

		// if count%100 == 0 {
		// 	fmt.Printf("read %d records\n", count)
		// }
	}
	// close(out)
	// close(e)
}
