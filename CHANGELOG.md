# Changelog

Notable user-visible changes are recorded here.

## Unreleased

### Fixed

- A `SELECT` that the server accepts makes its database the connection's, which `redis-conn-database` returns. The connection kept the database it was opened with, so a caller showing it went on showing that one.

## 0.1.1 - 2026-07-15

### Changed

- Connecting has a bounded asynchronous deadline, fragmented responses are scanned incrementally without repeatedly copying the whole buffer, a quit after a command was sent disconnects, and configurable limits bound responses, bulk strings, elements and nesting.
