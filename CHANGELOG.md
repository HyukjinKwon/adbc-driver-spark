# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Spark Connect protos pinned to v4.1.0, wire-compatible with Spark 3.5.x, 4.0.x,
  and 4.1.x, each exercised on every CI run against a live Spark Connect server.
- Runnable examples in Python, Go, C, R, Rust, and Ruby; the Python, C, R, Rust,
  and Ruby examples are validated end to end in CI against a live server.

[Unreleased]: https://github.com/HyukjinKwon/adbc-driver-spark/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HyukjinKwon/adbc-driver-spark/releases/tag/v0.1.0
