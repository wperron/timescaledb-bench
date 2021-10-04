# TimeScaleDB Benchmark

TimescaleDB query performance benchmark.

## Setup

You'll need at least Docker, docker-compose and `postgresql-client` installed
before starting.

you can spin up the database using the provided Makefile:

```bash
make start
make hydrate
```

## Running the benchmark

```
make run
```