<!-- SPDX-License-Identifier: Apache-2.0 -->
# Troubleshooting

Common problems connecting to and querying **Apache Spark Connect**, with what
each symptom usually means and how to fix it.

## Connection refused on port 15002

```
failed to connect to sc://localhost:15002: connection refused
```

The Spark Connect server is not running, or it is listening somewhere else.

- Confirm the server is up. Spark Connect listens on `sc://localhost:15002` by
  default. Start it with
  `./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.0.0`.
- Check the host and port in your URI. A managed endpoint usually uses port
  `443`, not `15002`.
- Verify nothing else is occupying the port and that no firewall blocks it.

## TLS and token errors

```
transport: authentication handshake failed
UNAUTHENTICATED: invalid or missing bearer token
```

- A bearer token requires TLS. The driver enables TLS automatically when a token
  is set, but if you set `use_ssl=false` explicitly with a token, the credential
  will not be sent. Remove the override or set `use_ssl=true`.
- For a plaintext local server, do not set `use_ssl=true`; the handshake will
  fail because the server has no TLS.
- Check the token has not expired and is the right kind for the endpoint
  (personal access token vs OAuth). See
  [Connecting and Authentication](connecting.md).

## "server session changed"

```
INVALID_ARGUMENT: server session id changed
```

The server restarted, or the session you pinned with `session_id` no longer
exists.

- Drop the `session_id` to let the driver create a fresh session.
- If you rely on session reuse, make sure the same server instance is still
  running and that the UUID is current.

## Large results stall or use too much memory

For very large result sets:

- Stream batches instead of materializing everything. Use
  `fetch_record_batch()` in Python or iterate the `RecordReader` in Go rather
  than `fetch_arrow_table()`. See [Querying Data](querying.md).
- Tune prefetch with the `adbc.rpc.result_queue_size` statement option to bound
  how many batches the driver buffers.
- If a long query drops mid-stream, the driver's reattachable execution resumes
  it automatically; a persistent failure usually points at a server-side or
  network timeout (`adbc.spark.connect.timeout_seconds`).

## CGO or shared library not found

```
cannot open shared object file: No such file or directory
AdbcDriverInit: symbol not found
```

This affects the C/C++/R/Python loading paths (the native Go driver does not use
the shared library).

- Make sure `libadbc_driver_spark.{so,dylib,dll}` is on the loader path
  (`LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`, or `PATH`), or pass its absolute path
  as the `driver` option.
- If you built from source, the C-ABI package must be built with
  `-tags driverlib -buildmode=c-shared` and a working cgo toolchain. See
  [Installation](installation.md).
- For Python, `pip install adbc-driver-spark` bundles the library; a "not found"
  error usually means a partial or source install without the compiled artifact.

## Version mismatches

```
Arrow IPC: unsupported metadata version
proto: cannot parse invalid wire-format data
```

- Check the Spark Connect server version. The driver supports Spark 4.0.x and
  4.1.x (protos pinned to v4.1.0). See [Compatibility](compatibility.md).
- For Go, the module requires Go 1.25 or newer and matched `arrow-go` and
  `arrow-adbc` versions; run `go mod tidy` if you see resolution errors.
- For Python, ensure `pyarrow` and `adbc-driver-manager` were installed by the
  same `pip install` so their ABIs line up.

!!! tip
    Set the gRPC log level on the server, or capture the full ADBC error
    message (the driver propagates the server's status and detail), to see the
    underlying Spark Connect error behind a generic failure.
