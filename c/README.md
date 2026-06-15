# C-ABI shared library (`libadbc_driver_spark`)

This directory builds the Spark Connect driver into a C-ABI shared library that
the [ADBC Driver Manager](https://arrow.apache.org/adbc/) can load from any
language (C/C++, Python, R, Ruby, Rust, ...). It is the packaging layer (M3) on
top of the native Go driver in [`../driver/spark`](../driver/spark).

## What is here

| File | Origin | Purpose |
|------|--------|---------|
| `driver.go` | generated | cgo wrappers implementing the full ADBC C ABI, delegating to `spark.NewDriver`. Exports `AdbcDriverInit` (standard) and `AdbcDriverSparkInit`. |
| `utils.c`, `utils.h` | generated | C helpers used by `driver.go`. |
| `arrow-adbc/adbc.h` | vendored | the ADBC C API header (Apache Arrow ADBC, pinned to go/adbc v1.11.0). |

`driver.go`, `utils.c`, and `utils.h` are generated and carry a
`// Code generated ... DO NOT EDIT.` header. They are checked in so the library
builds without a code-generation step in CI.

## Build

The package is behind the `driverlib` build tag and uses cgo, so it must be
built with `-tags driverlib -buildmode=c-shared`:

```bash
# from the repository root
go build -tags driverlib -buildmode=c-shared \
  -ldflags "-s -w -X github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase.infoDriverVersion=$(cat python/adbc_driver_spark/_version.py | sed -n 's/.*__version__ = "\(.*\)"/\1/p')" \
  -o libadbc_driver_spark.dylib ./c     # .so on Linux, .dll on Windows
```

The `Makefile` target `c-lib` wraps this and picks the correct extension per OS.

The resulting library exports the standard `AdbcDriverInit` entrypoint, so the
Python package (and any ADBC driver manager) loads it without naming an
entrypoint. `AdbcDriverSparkInit` is also exported for callers that prefer the
driver-specific symbol.

## Regenerating

The generated files come from the Apache Arrow ADBC `pkg` generator, pinned to
the same version as the `github.com/apache/arrow-adbc/go/adbc` module in
`go.mod` (currently v1.11.0):

```bash
go run <arrow-adbc>/go/adbc/pkg/gen/main.go \
  -prefix Spark -driver ./driver/spark -o ./c -in <arrow-adbc>/go/adbc/pkg/_tmpl
# then rewrite the header include path to "arrow-adbc/adbc.h"
```

Keep the generator version in lockstep with the `arrow-adbc/go/adbc` dependency:
the generated cgo code matches that version's C ABI exactly.

## Notes for CI (M4)

`go vet -tags driverlib ./c` reports a few `non-constant format string` findings
inside the generated `driver.go`. These come straight from the upstream
arrow-adbc template (the code is `DO NOT EDIT`), so exclude generated files from
the vet gate or treat them as known/accepted rather than editing the generated
source.
