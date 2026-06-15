<!-- SPDX-License-Identifier: Apache-2.0 -->
# Using from C and C++

C and C++ programs load `libadbc_driver_spark` through the ADBC driver manager
and drive it with the plain ADBC C API declared in `adbc.h`. The library exports
the standard `AdbcDriverInit` entrypoint, so the driver manager resolves it
without you naming a symbol.

## Prerequisites

- The shared library `libadbc_driver_spark.{so,dylib,dll}`, either downloaded
  from [Releases](https://github.com/HyukjinKwon/adbc-driver-spark/releases) or
  built from source (see [Installation](installation.md)).
- The ADBC driver manager (`libadbc_driver_manager`) and `adbc.h`. The header
  shipped with this project lives at `c/arrow-adbc/adbc.h`.

## Minimal example

This program connects to a local Spark Connect server, runs a query, and prints
the row count of each Arrow batch.

```c
// SPDX-License-Identifier: Apache-2.0
#include <stdio.h>
#include <string.h>

#include <arrow-adbc/adbc.h>

static int check(AdbcStatusCode code, struct AdbcError* error, const char* what) {
  if (code != ADBC_STATUS_OK) {
    fprintf(stderr, "%s failed: %s\n", what,
            error->message ? error->message : "(no message)");
    if (error->release) error->release(error);
    return 1;
  }
  return 0;
}

int main(void) {
  struct AdbcError error = {0};
  struct AdbcDatabase database = {0};
  struct AdbcConnection connection = {0};
  struct AdbcStatement statement = {0};

  // 1. Database: point the driver manager at the shared library and set the URI.
  if (check(AdbcDatabaseNew(&database, &error), &error, "AdbcDatabaseNew")) return 1;
  AdbcDatabaseSetOption(&database, "driver", "adbc_driver_spark", &error);
  AdbcDatabaseSetOption(&database, "uri", "sc://localhost:15002", &error);
  if (check(AdbcDatabaseInit(&database, &error), &error, "AdbcDatabaseInit")) return 1;

  // 2. Connection.
  if (check(AdbcConnectionNew(&connection, &error), &error, "AdbcConnectionNew")) return 1;
  if (check(AdbcConnectionInit(&connection, &database, &error), &error,
            "AdbcConnectionInit")) return 1;

  // 3. Statement: run a query.
  if (check(AdbcStatementNew(&connection, &statement, &error), &error,
            "AdbcStatementNew")) return 1;
  AdbcStatementSetSqlQuery(&statement, "SELECT id, id * id AS square FROM range(5)", &error);

  struct ArrowArrayStream stream = {0};
  int64_t rows_affected = -1;
  if (check(AdbcStatementExecuteQuery(&statement, &stream, &rows_affected, &error),
            &error, "AdbcStatementExecuteQuery")) return 1;

  // 4. Read Arrow batches from the stream.
  struct ArrowSchema schema = {0};
  stream.get_schema(&stream, &schema);
  struct ArrowArray batch = {0};
  while (stream.get_next(&stream, &batch) == 0 && batch.release != NULL) {
    printf("batch with %lld rows\n", (long long)batch.length);
    batch.release(&batch);
  }
  if (schema.release) schema.release(&schema);
  if (stream.release) stream.release(&stream);

  // 5. Clean up.
  AdbcStatementRelease(&statement, &error);
  AdbcConnectionRelease(&connection, &error);
  AdbcDatabaseRelease(&database, &error);
  return 0;
}
```

## Building

Compile against the driver manager and link it. Adjust include and library paths
to where you placed the headers and libraries.

```bash
cc example.c -o example \
  -I/path/to/include \
  -L/path/to/lib -ladbc_driver_manager
```

At run time, the loader must be able to find both `libadbc_driver_manager` and
`libadbc_driver_spark`:

```bash
# Linux
LD_LIBRARY_PATH=/path/to/lib ./example
# macOS
DYLD_LIBRARY_PATH=/path/to/lib ./example
```

!!! tip
    Instead of the bare name `adbc_driver_spark`, you can pass the library's
    absolute path as the `driver` option, for example
    `/opt/adbc/libadbc_driver_spark.so`. This avoids loader-path issues.

!!! note
    Authentication and other settings are plain database options. To use a
    bearer token over TLS, add
    `AdbcDatabaseSetOption(&database, "adbc.spark.connect.token", "...", &error)`
    and `"adbc.spark.connect.use_ssl", "true"`. See the
    [Configuration Reference](configuration.md).
