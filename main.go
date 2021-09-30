package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v4"
)

var (
	host = flag.String("host", "", "TimescaleDB hostname and port number")
	user = flag.String("user", "", "TimescaleDB username")
	pwd  = flag.String("password", "", "TimescaleSB password")
	db   = flag.String("db", "", "TimescaleDB database name")
	file = flag.String("file", "", "Source data filename")
)

const (
	insert = `INSERT INTO cpu_usage (ts, host, usage) VALUES ($1, $2, $3)`
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
		log.Fatalf("opening source data file: %s", err)
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

		_, err = conn.Exec(ctx, insert, rec[0], rec[1], rec[2])
		if err != nil {
			log.Fatalf("inserting into database: %s", err)
		}
	}
}
