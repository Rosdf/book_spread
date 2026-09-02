# all_venues

The one enum every other crate in the workspace names a venue by:

```rust
pub enum Venue {
    BinanceSpot,
    Bitstamp,
}
```

It is a crate of its own, not part of `core_lib`, because it has to sit *below* `core_lib` in
the dependency graph. `core_lib` tags every `Instrument` it interns with a `Venue`, and each
venue crate (`binance_spot`, `bitstamp`) names its own `Venue` variant on its `Connector` impl.
Putting the enum in `core_lib` would satisfy the first of those and break the second: a venue
crate already depends on `core_lib`, so `core_lib` cannot depend back on a venue crate for its
own identity type. A crate with no dependencies of its own is what both directions can point
at.

`Venue::ALL` and `Venue::COUNT` are written out by hand rather than derived, so a `match` over
every venue - `Venue::as_str`, a server's per-venue connector table - stops compiling the day a
variant is added, rather than silently leaving it unhandled.

Not `#[non_exhaustive]`, on purpose: every venue this build carries is listed here.
