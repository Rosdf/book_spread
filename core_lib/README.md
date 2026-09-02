# core_lib

Everything a venue connector needs beyond its own wire format and sequencing rules: the generic
connection loop and supervisor, the instrument registry, the order-book types, and the
lock-free single-producer/single-consumer channel a book is read through.

Depends only on `all_venues` (for the `Venue` enum) - a venue crate (`binance_spot`, `bitstamp`)
depends on this crate, never the other way around.

## What lives here vs. in a venue crate

`core_lib::venue` used to hold only the truly venue-agnostic pieces - retry pacing, closing a
socket by the book, a scratch buffer for in-place JSON parsing - while the connection loop, the
slot state machine, and the supervisor glue each lived in their own venue crate. Comparing
`binance_spot` and `bitstamp` once both existed showed that split cost more than it saved:
`connection.rs` was 55% identical text between them, `table.rs` and `rest.rs` nearly all of it.
So this module now owns the connection loop, the slot table, the supervisor, the REST snapshot
fetch, and the hourly symbol listing - all generic over `venue::spec::VenueSpec`. A venue crate
keeps its own decoder, its wire naming, its config extras, and one `impl VenueSpec` wiring the
two together.

`VenueSpec` deliberately carries no transport generics (`RestClient`, `WsConnector`) of its
own - see `venue::spec`'s module doc for why keeping decode and sequencing logic free of
transport types is what makes it unit-testable against plain JSON fixtures, no socket in sight.

## The book types

- **`incremental_book::IncrementalBook`** - the full-depth book a venue's diffs are applied to.
  Reports which tier of the book a diff touched (`Close`/`Deep`/`Both`), so a connector can skip
  publishing when only the deep tail moved.
- **`small_book::SmallBook`** - the published shape: the best 10 levels each side, `heapless` so
  publishing allocates nothing.
- **`positive_f64::PositiveF64`** - an ordered, non-negative `f64` with no accessor. A price or
  size comes out of a `SmallBook` as this type; nothing in this codebase needs the inner value,
  so nothing is given a way to get at it by accident.

## The publish/read pair

`connector::book_publisher::{BookPublisher, BookReader}` are the two halves of a
`shared_buffer` pair for the buffering, wired to the two halves of an `atomic_waker` pair for
parking - one producer, one consumer, no lock. A reader that falls behind does not queue: it
silently misses intermediate books and sees only the newest one on its next `wait_update`. On
drop, the publisher writes an empty `SmallBook` as the in-band shutdown sentinel, so a reader
parked in `wait_update` is woken with `Some(())` once and gets `None` from then on rather than
parking forever.

`shared_buffer` and `atomic_waker` are also the two modules checked under
[loom](https://docs.rs/loom) (`--cfg loom`) - every other module here reaches the network in
some form, which loom's model of `tokio` cannot compile.

## The instrument registry

`instrument::Instrument` replaces what used to be a `String` re-validated and re-cloned at every
layer with a `Copy` handle into a registry this module leaks records into exactly once: identity
is the record's address, so equality and hashing are address comparisons. Nothing outside this
module can mint one - the only way in is `connector::InstrumentRegistrar::register`, reachable
only through the sealed guard a connector is handed at spawn, so a venue's decoder cannot
register an instrument under another venue's name by mistake.

## Where the generic connector lives

| Module | Responsibility |
|---|---|
| `connector` | `ConnectorHandle`, the public entry point a caller subscribes a symbol through; `book_publisher` |
| `instrument` | The interned `Instrument` handle and its process-global registry |
| `venue::spec` | The `VenueSpec` trait a venue implements, plus `FrameAction`, `Retry`, `ControlPacer`, `Decoder` |
| `venue::connection` | One socket: the session loop, admitting symbols, bootstrap and its recoveries, watchdogs, backoff |
| `venue::supervisor` | Reads the subscription queue, checks a symbol against the listing, routes it to a connection with room |
| `venue::router` / `venue::table` | Symbol -> connection routing; the per-symbol slot state machine |
| `venue::pending` | `PendingDiffs` - what a venue's bootstrap arena has to offer while a symbol has no book yet |
| `venue::levels` | Decimal and price-level decoding shared across venues |
| `venue::rest` / `venue::universe` | The snapshot fetch and its concurrency limit; the periodic symbol listing |
| `venue::config` | `CoreConfig`, `ConnectorConfig<V>`, `Defaults` |
| `venue::session` / `venue::backoff` / `venue::scratch` | Session end and the close handshake; retry pacing; the reused JSON scratch buffer |
| `incremental_book`, `small_book`, `positive_f64` | The book types above |
| `atomic_waker`, `shared_buffer` | The lock-free primitives the publish/read pair is built from |
| `map` | The hasher/map aliases used on the per-frame hot path - see `venue::table`'s doc for why not the standard one |

See `venues/binance_spot`'s README for a worked example of how a venue crate uses all of this,
and its "Where the code lives" section for the exact file-by-file split.
