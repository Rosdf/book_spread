# md_wire

The gRPC contract for `md.v1.MarketData` - the paths, framing constants, and refusal codes both
the server (`md_server`) and its clients build against. Message *types* live in `md_proto`; this
crate is the handful of things two independently-written ends have to agree on that a `.proto`
file doesn't capture on its own.

It is ordinary gRPC over HTTP/2 - h2 on the server side, tonic on the client side own the
frames - so most of the protocol is not spelled out here. What is:

- **Paths**: `SUBSCRIBE_PATH` and `CATALOGUE_PATH`, `POST`, with a `content-type` starting
  `CONTENT_TYPE_PREFIX`.
- **Stream shape**: a request is one length-prefixed message followed by `END_STREAM`; a
  subscribe stream is one length-prefixed `BookUpdate` per DATA frame; a catalogue call is a
  single message followed by `grpc-status: 0` trailers.
- **Refusal**: a Trailers-Only response carrying `grpc-status`, `grpc-message`, and the exact
  `RejectCode` in `REJECT_CODE_HEADER`.

## Two index spaces

A `CatalogueRequest` reply hands back two tables: `VenueIdx -> venue name`, and
`CatalogueIdx -> the pairs under it`. Everything on the hot path after that is numeric - no book
update spells a symbol out. The one exception is a subscribe: it names a `CatalogueIdx`, but
also every pair the client read under it, because that index is a position in a file the server
can be restarted on after an edit. See `md_server::catalogue` and
`md_server::registry::Registry::subscribe` for what a mismatch there means.

## `RejectCode`

Nine of them, each carrying more than the canonical gRPC status can. In particular,
`RejectCode::retryable` is not derivable from the status alone - `NOT_FOUND` covers both an
instrument this server will never carry (`UnknownInstrument`, permanent) and one whose venue
just hasn't listed the symbol yet (`UnlistedSymbol`, worth retrying). A client that only speaks
gRPC still gets a sensible status via `RejectCode::status`; one that reads
`REJECT_CODE_HEADER` gets to tell the two apart.

## One book per stream

A request names one instrument, and every pair under it is validated even though only the first
is served on the wire - a client that wants three symbols opens three streams. HTTP/2 would
happily multiplex them onto one connection, but `md_server` keeps one stream per connection so a
broadcaster can own its clients' connections outright: no mutex, no interleaving between
symbols, no head-of-line blocking between them.
