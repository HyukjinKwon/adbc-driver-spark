<!-- SPDX-License-Identifier: Apache-2.0 -->
# Go examples

Programs that use the native Go ADBC driver for Spark Connect. Each example is
its own `main` package in a subdirectory, so they build and run independently.

## Examples

| Directory | What it shows |
| --- | --- |
| [`quickstart/`](quickstart) | Open a database, run a `SELECT`, and iterate the Arrow `RecordReader`. |
| [`metadata/`](metadata) | `GetObjects`, `GetTableSchema`, and `GetTableTypes` for catalog introspection. |
| [`parameters/`](parameters) | `Prepare` a statement and `Bind` positional parameters from an Arrow record. |

## Run

Start a Spark Connect server reachable at `sc://localhost:15002`, then run any
example by its package path:

```bash
go run ./examples/go/quickstart
go run ./examples/go/metadata
go run ./examples/go/parameters
```

The endpoint defaults to `sc://localhost:15002`. Set `SPARK_REMOTE` (or
`SPARK_CONNECT_URI`) to point at another server, including auth options:

```bash
SPARK_REMOTE='sc://spark.example.com:443/;token=<jwt>;use_ssl=true' \
    go run ./examples/go/quickstart
```
