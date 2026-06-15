<!-- SPDX-License-Identifier: Apache-2.0 -->
# Installation

The driver is built once in Go, compiled to a C-ABI shared library, and consumed
from every language with an ADBC driver manager. Pick the install path that
matches your language.

## Python

```bash
pip install adbc-driver-spark
```

The wheel bundles the prebuilt `libadbc_driver_spark` shared library for your
platform, plus `adbc-driver-manager` and `pyarrow`. No compiler or Go toolchain
is required.

!!! tip
    For DataFrame helpers, install pandas and/or Polars alongside it:
    `pip install adbc-driver-spark pandas polars`.

Supported Python versions are 3.9 through 3.13 on Linux (x86_64, aarch64), macOS
(x86_64, arm64), and Windows (x86_64).

## Go

```bash
go get github.com/HyukjinKwon/adbc-driver-spark
```

This pulls in the native Go driver under
`github.com/HyukjinKwon/adbc-driver-spark/driver/spark`, together with
`github.com/apache/arrow-adbc/go/adbc` and `github.com/apache/arrow-go/v18`.
Go 1.25 or newer is required. See [Using from Go](usage-go.md).

## C, C++, and R

These languages load the shared library through the ADBC driver manager. You can
either download a release binary or build from source.

### Download a release binary

Release assets are per-platform tarballs on the
[Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) page. Each
tarball extracts to the current directory and contains
`libadbc_driver_spark.{so,dylib,dll}` plus LICENSE and NOTICE. Download the asset
for your platform and extract it, then point your ADBC driver manager at the
extracted library:

```bash
# Download the shared library for your platform from the Releases page
curl -fsSL -o adbc-spark.tar.gz \
  https://github.com/HyukjinKwon/adbc-driver-spark/releases/latest/download/libadbc_driver_spark-linux-x86_64.tar.gz
tar xzf adbc-spark.tar.gz
export SPARK_DRIVER="$PWD/libadbc_driver_spark.so"   # .dylib on macOS, .dll on Windows
```

| Platform        | Asset |
|-----------------|-------|
| Linux x86_64    | `libadbc_driver_spark-linux-x86_64.tar.gz` |
| Linux aarch64   | `libadbc_driver_spark-linux-aarch64.tar.gz` |
| macOS x86_64    | `libadbc_driver_spark-macos-x86_64.tar.gz` |
| macOS arm64     | `libadbc_driver_spark-macos-arm64.tar.gz` |
| Windows x86_64  | `libadbc_driver_spark-windows-x86_64.tar.gz` |

See [Using from C and C++](usage-c.md) and [Using from R](usage-r.md) for how to
load the library.

### Build the shared library from source

You need Go 1.25 or newer and a C toolchain (clang or gcc), because the C-ABI
layer uses cgo.

```bash
git clone https://github.com/HyukjinKwon/adbc-driver-spark.git
cd adbc-driver-spark

# Build the shared library. The C-ABI package is behind the `driverlib`
# build tag and uses cgo, so build with -buildmode=c-shared.
go build -tags driverlib -buildmode=c-shared \
  -o libadbc_driver_spark.dylib ./c   # .so on Linux, .dll on Windows
```

A `Makefile` target wraps this and picks the correct file extension per OS:

```bash
make c-lib
```

!!! warning
    Cross-compiling a cgo `c-shared` library requires a matching C cross
    toolchain. Build on the target platform unless you have one configured.

Once built, place the library somewhere on your loader path
(`LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH` on macOS, `PATH` on Windows),
or pass its absolute path to the driver manager.

Once installed, head to the [Quickstart](quickstart.md).
