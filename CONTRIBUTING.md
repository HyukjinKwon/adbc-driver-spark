# Contributing

Thanks for your interest in improving the ADBC driver for Spark Connect. This
document describes how to set up a development environment, run the tests, and
submit changes.

## Project layout

```
proto/spark/connect/   Vendored Spark Connect protobufs (pinned to Spark 4.0.0)
internal/sparkconnect/  gRPC client + generated stubs + Arrow decoding
driver/spark/           ADBC driver (database, connection, statement, metadata)
c/                      cgo C-ABI export -> libadbc_driver_spark.{so,dylib,dll}
python/                 Python package (adbc_driver_spark) + DBAPI 2.0 layer
examples/               Runnable examples for Go, Python, C, and R
tests/                  Go and Python integration tests, docker compose
docs/ + mkdocs.yml      MkDocs Material documentation site
```

## Prerequisites

- Go 1.25 or newer (the Arrow Go and ADBC modules require it).
- Python 3.9 or newer, for the Python package and tests.
- A C toolchain (cgo) to build the shared library: clang or gcc.
- `buf` 1.31+ if you need to regenerate the Spark Connect stubs.
- Docker, to run the integration tests against a real Spark Connect server.

## Building

```bash
# Build the Go driver and run unit tests.
go build ./...
go test ./...

# Build the C-ABI shared library.
make libadbc_driver_spark

# Build and install the Python package in editable mode.
pip install -e "python/[test]"
```

If your environment cannot reach `proxy.golang.org`, set
`GOPROXY=https://goproxy.io,direct` and `GOTOOLCHAIN=auto`.

## Regenerating the Spark Connect stubs

The `.proto` files under `proto/spark/connect/` are vendored from Apache Spark.
To regenerate the Go stubs after a proto change:

```bash
buf generate
```

Do not edit files under `internal/sparkconnect/` that are produced by codegen.

## Running the tests

Unit tests run without a server:

```bash
go test ./...
pytest python/tests -k "not integration"
```

Integration tests need a Spark Connect server. The compose file in `tests/`
starts one:

```bash
docker compose -f tests/docker-compose.yml up -d
SPARK_CONNECT_URI=sc://localhost:15002 go test ./... -tags=integration
SPARK_CONNECT_URI=sc://localhost:15002 pytest python/tests
docker compose -f tests/docker-compose.yml down
```

## Documentation

The docs are built with MkDocs Material.

```bash
pip install mkdocs-material
mkdocs serve     # live preview at http://127.0.0.1:8000
mkdocs build --strict
```

## Style

- Go code is formatted with `gofmt` and vetted with `go vet` and
  `golangci-lint run`.
- Python code is formatted and linted with `ruff` and type checked with `mypy`.
- Keep prose plain. Use regular ASCII punctuation, not em dashes or other fancy
  characters.

## Submitting changes

1. Fork the repository and create a topic branch.
2. Make your change with tests and documentation updates.
3. Run the relevant linters and tests locally.
4. Open a pull request with a clear description of the change and its motivation.

All contributions are made under the Apache License 2.0. By participating you
agree to abide by the [Code of Conduct](CODE_OF_CONDUCT.md).
