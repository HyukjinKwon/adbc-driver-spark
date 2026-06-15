# Test infrastructure

This directory holds the integration and end-to-end (e2e) test infrastructure
for `adbc-driver-spark`. Fast unit tests live next to the code they cover (Go
tests under `driver/`, `internal/`, etc., and Python tests under `python/tests/`).
The suites here exercise the driver against a **real Spark Connect server**.

## Layout

```
tests/
  docker/        Self-contained Apache Spark Connect server (Docker)
    Dockerfile
    docker-compose.yml
    entrypoint.sh
  e2e/           End-to-end suites (require a running server)
    harness.go            shared Go helpers (build tag: e2e)
    query_test.go         queries + Spark->Arrow type mapping
    conformance_test.go   metadata, prepared/bind, errors, transactions
    conftest.py           pytest fixtures (skip when unavailable)
    test_python_e2e.py    Python DBAPI 2.0 e2e
    requirements.txt
```

## Start a Spark Connect server

```bash
docker compose -f tests/docker/docker-compose.yml up --build -d
# Server is ready when the health check passes (gRPC on port 15002).
# Connect with the URI:  sc://localhost:15002
docker compose -f tests/docker/docker-compose.yml down   # when finished
```

Override the Spark version with `SPARK_VERSION=4.1.2` (default) /
`SCALA_VERSION=2.13`. Any published `apache/spark` tag works (e.g. `4.0.3`).

## Run the Go e2e suite

The Go e2e tests are gated behind the `e2e` build tag and skip at runtime unless
`SPARK_CONNECT_URI` is set, so they never interfere with `go test ./...`.

```bash
SPARK_CONNECT_URI=sc://localhost:15002 go test -tags e2e -v ./tests/e2e/...
```

## Run the Python e2e suite

Install the built wheel (so the bundled shared library is importable), then:

```bash
pip install -r tests/e2e/requirements.txt
pip install dist/adbc_driver_spark-*.whl
SPARK_CONNECT_URI=sc://localhost:15002 pytest -v tests/e2e/
```

## In CI

The [`e2e` workflow](../.github/workflows/e2e.yml) runs the full suite against a
**matrix of Spark versions** to guarantee compatibility: the latest patch of
each supported minor line (currently **4.1.2** and **4.0.3**). Following the
same approach as the `spark-connect-scala3` and `spark-connect-ruby` clients,
CI downloads the official Spark distribution onto the runner and starts its
bundled Connect server (no Docker image required), then builds the driver +
Python wheel and runs both the Go and Python suites. It runs on pushes to the
default branch, on relevant pull requests, nightly, and on demand
(`workflow_dispatch`).

The Docker harness above is a convenience for local development. Because no
Docker daemon is available in every developer sandbox, all suites degrade
gracefully to a skip when no server is reachable (`SPARK_CONNECT_URI` unset).
