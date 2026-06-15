# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-06-15

### Added

- Streaming result delivery: the driver now decodes one Arrow batch at a time
  and hands it to the consumer on demand, holding only the current batch in
  memory instead of buffering the entire result. Memory stays flat for
  arbitrarily large results, and abandoning a reader early cancels the server
  operation.
- `adbc.spark.headers.<NAME>` database option to set arbitrary gRPC metadata
  headers, at parity with the connection-string behavior. Exposed in Python as
  `DatabaseOptions.HEADER_PREFIX`.
- Parameter binding for `timestamp`, `timestamp_ntz`, `decimal` (decimal128 and
  decimal256), and `date64` Arrow types.

### Fixed

- Out-of-bounds write in the C-ABI option path that could panic on the standard
  two-call length-probe idiom used to read a string option.
- Connection-string values containing `+` (bearer tokens, base64 secrets, JWTs)
  were corrupted into spaces; they are now preserved.
- Unknown `adbc.spark.*` options were silently ignored; they now return an error
  so a misspelled key (for example a token under the wrong name) is not lost.
- Possible panic when calling metadata methods or creating a statement after the
  connection was closed; these now return a clear invalid-state error.
- `GetObjects` now honors the table-type filter, reports temporary views as
  `TEMPORARY`, and applies the column-name filter.
- `GetInfo` now reports the Spark server version.
- gRPC `Unauthenticated` now maps to the unauthenticated ADBC status rather than
  unauthorized.
- `uint64` bind values above the signed 64-bit maximum are rejected instead of
  wrapping to a negative long.
- Session release on close is bounded by a timeout so close cannot hang.
- The Python DBAPI layer forwards `conn_kwargs` so a cloned connection keeps its
  configuration.

### Changed

- Documentation corrected to match the driver: the real `adbc.spark.*` option
  keys (previously documented as `adbc.spark.connect.*`), the TLS key
  `adbc.spark.tls.enabled`, the Spark Connect proto pin documented as v4.1.2,
  and removal of options and features that were never implemented (the
  `adbc.connection.*` and `adbc.rpc.result_queue_size` option keys and the
  reattachable-execution description).
- CI actions updated to their Node 24 majors.

### Known limitations

- No automatic reconnection or retry on transient failures, and no server-side
  statement interrupt (client-side context cancellation only).

## [0.1.0] - 2026-06-15

Initial release of the Apache Arrow ADBC driver for Apache Spark Connect.

### Added

- ADBC driver for Apache Spark Connect.
- Go core with a C-ABI shared library exposing the standard ADBC entrypoint (`AdbcDriverInit`).
- Python package `adbc-driver-spark` with a DBAPI 2.0 (PEP 249) interface.
- SQL execution that returns results as Apache Arrow.
- Metadata introspection for catalogs, schemas, tables, and columns.
- Prepared statements with positional parameter binding.
- TLS and bearer-token authentication.
- Spark Connect protos pinned to v4.1.2, wire-compatible with Spark 3.5.x, 4.0.x,
  and 4.1.x, each exercised on every CI run against a live Spark Connect server.
- Runnable examples in Python, Go, C, R, Rust, and Ruby; the Python, C, R, Rust,
  and Ruby examples are validated end to end in CI against a live server.

[Unreleased]: https://github.com/HyukjinKwon/adbc-driver-spark/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/HyukjinKwon/adbc-driver-spark/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/HyukjinKwon/adbc-driver-spark/releases/tag/v0.1.0
