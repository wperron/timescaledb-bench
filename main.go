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

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("reading record from csv: %s", err)
		}

		// start, err := time.Parse(pattern, rec[1])
		// if err != nil {
		// 	log.Fatalf("parsing timestamp: %s", err)
		// }
		// end, err := time.Parse(pattern, rec[2])
		// if err != nil {
		// 	log.Fatalf("parsing timestamp: %s", err)
		// }

		rows, err := conn.Query(ctx, q, rec[0], rec[1], rec[2])
		if err != nil {
			log.Fatalf("querying database: %s", err)
		}

		hostname := ""
		ts := time.Time{}
		var max, min float64
		for rows.Next() {
			rows.Scan(&hostname, &ts, &max, &min)
			fmt.Println(hostname, ts, max, min)
		}
	}
}
