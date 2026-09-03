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

---

## Ways to make market data faster

None of these are implemented yet. They are ideas for cutting latency further, noted here
because they cut across venue connectors rather than belonging to any one of them.

- **Connection arbitrage.** Run more than one connection to the same venue for the same symbols
  and take whichever delivers an update first. Network paths and server-side fan-out are not
  perfectly uniform, so redundant connections can shave tail latency at the cost of duplicate
  work.
- **Endpoint-speed arbitrage.** Some venues expose the same data at different update cadences on
  different endpoints (e.g. Binance's `@depth` vs `@depth@100ms` vs `@depth@0ms`, or separate
  REST/WS tiers). Racing the fastest available endpoint per venue, rather than picking one
  cadence up front, would give the freshest data at each moment.
- **BBO/L2 arbitrage.** A venue's dedicated best-bid/offer stream often updates faster than its
  full L2 depth stream, since it carries less data. Taking top-of-book from whichever of the two
  arrives first, and only trusting L2 for the rest of the book, would tighten top-of-book
  latency without giving up depth.
- **Folding anonymous deals into the book.** Trade/deal prints (anonymous, no side-by-side book
  context) usually lag or lead a book update slightly. Feeding them into the merged book as a
  same-price signal - rather than only consuming diffs - could catch a price move an update
  frame hasn't reflected yet.
