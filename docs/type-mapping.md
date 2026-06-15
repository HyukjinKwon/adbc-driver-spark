<!-- SPDX-License-Identifier: Apache-2.0 -->
# Type Mapping

Spark Connect returns results as Arrow IPC batches, so the driver maps Spark SQL
types to Arrow types directly. This page is the reference for that mapping in
both directions (result columns and bound parameters).

## Spark to Arrow

| Spark SQL type      | Arrow type                  |
|---------------------|-----------------------------|
| `boolean`           | `bool`                      |
| `byte` (tinyint)    | `int8`                      |
| `short` (smallint)  | `int16`                     |
| `int` (integer)     | `int32`                     |
| `long` (bigint)     | `int64`                     |
| `float`             | `float32`                   |
| `double`            | `float64`                   |
| `decimal(p, s)`     | `decimal128(p, s)`          |
| `string`            | `utf8`                      |
| `binary`            | `binary`                    |
| `date`              | `date32`                    |
| `timestamp`         | `timestamp[us, tz]`         |
| `timestamp_ntz`     | `timestamp[us]` (no zone)   |
| `array<T>`          | `list<T>`                   |
| `map<K, V>`         | `map<K, V>`                 |
| `struct<...>`       | `struct<...>`               |
| `null`              | `null`                      |

## Notes on specific types

### Timestamps

Spark has two timestamp types and they map differently:

- `timestamp` is an instant and carries a time zone. It maps to Arrow
  `timestamp[us, tz]`, where the zone reflects the session time zone.
- `timestamp_ntz` (no time zone) is wall-clock time. It maps to Arrow
  `timestamp[us]` with no zone attached.

Both use microsecond precision, matching Spark's internal representation.

!!! note
    The effective time zone for `timestamp` follows the session's
    `spark.sql.session.timeZone`. Set it through a connection configuration
    option if you need a specific zone.

### Decimal

`decimal(p, s)` maps to Arrow `decimal128(p, s)` with the same precision and
scale, so values are exact (no floating point rounding).

### Nested types

`array`, `map`, and `struct` map to the corresponding Arrow nested types and can
be composed arbitrarily (for example `array<struct<...>>`). Field names and
nullability are preserved.

## Parameter binding

When you bind parameters, build an Arrow record whose column types match the
Spark types expected by the query, using the same mapping in reverse. For
example bind an `int64` array for a `bigint` column and a `decimal128(p, s)`
array for a `decimal(p, s)` column. See [Querying Data](querying.md) for binding
examples.

!!! tip
    Because the mapping is one to one and columnar, results from this driver
    drop straight into pandas, Polars, or DuckDB through `pyarrow` without any
    per-row conversion.
