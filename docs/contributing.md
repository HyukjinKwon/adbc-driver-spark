<!-- SPDX-License-Identifier: Apache-2.0 -->
# Contributing

Contributions are welcome. The authoritative guide is
[CONTRIBUTING.md](https://github.com/HyukjinKwon/adbc-driver-spark/blob/main/CONTRIBUTING.md)
in the repository; this page is a short orientation.

## Prerequisites

- Go 1.25 or newer (the Arrow Go and ADBC modules require it).
- Python 3.9 or newer, for the Python package and tests.
- A C toolchain (cgo) to build the shared library: clang or gcc.
- `buf` 1.31 or newer, if you need to regenerate the Spark Connect stubs.
- Docker, to run integration tests against a real Spark Connect server.

## Building

```bash
# Go driver and unit tests.
go build ./...
go test ./...

# C-ABI shared library.
make c-lib

# Python package in editable mode.
pip install -e "python/[test]"
```

## Testing

Unit tests run without a server:

```bash
go test ./...
pytest python/tests -k "not integration"
```

End-to-end tests need a Spark Connect server. The compose file in `tests/`
starts one, then point the tests at it:

```bash
docker compose -f tests/docker-compose.yml up -d
SPARK_CONNECT_URI=sc://localhost:15002 go test ./... -tags=integration
SPARK_CONNECT_URI=sc://localhost:15002 pytest python/tests
docker compose -f tests/docker-compose.yml down
```

## Regenerating protos with buf

The `.proto` files under `proto/spark/connect/` are vendored from Apache Spark.
After a proto change, regenerate the Go stubs with buf:

```bash
buf generate
```

Do not hand-edit generated files under `internal/sparkconnect/` or the
`c/driver.go`, `c/utils.c`, `c/utils.h` cgo wrappers; they carry a
`DO NOT EDIT` header.

## Style

- Go is formatted with `gofmt` and vetted with `go vet` and `golangci-lint run`.
- Python is formatted and linted with `ruff` and type checked with `mypy`.
- Keep prose plain. Use regular ASCII punctuation, not em dashes or other fancy
  characters.

## Docs

This site is built with MkDocs Material:

```bash
pip install mkdocs-material
mkdocs serve              # live preview at http://127.0.0.1:8000
mkdocs build --strict
```

## Submitting changes

1. Fork the repository and create a topic branch.
2. Make your change with tests and documentation updates.
3. Run the relevant linters and tests locally.
4. Open a pull request with a clear description of the change and its motivation.

All contributions are made under the Apache License 2.0. By participating you
agree to abide by the project's Code of Conduct.
