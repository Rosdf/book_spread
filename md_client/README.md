# md_client

A gRPC client for `md.v1.MarketData`: `catalogue`, `sub`, `unsub` at an interactive prompt,
watching one instrument's merged book at a time.

```sh
cargo run -p md_client                                          # starts at the prompt
cargo run -p md_client -- binance_spot BTCUSDT                   # starts already subscribed
cargo run -p md_client -- --addr 127.0.0.1:50051 binance_spot BTCUSDT
```

`--addr <addr>` names the server, in either order relative to `<venue> <symbol>`; without it,
`127.0.0.1:50051`.

## Commands

| Command | Alias | Does |
|---|---|---|
| `catalogue` | `c` | Fetch the catalogue and list every instrument it carries, id and pairs |
| `sub <id>` | `s <id>` | Subscribe to the instrument at `<id>`, replacing any current subscription |
| `unsub` | `u` | Drop the current subscription |
| `help` | | Print the command list |
| `quit` | `q` | Exit |

One subscription at a time - a new `sub` replaces whatever was running, the way opening a
second book on the same terminal would. `<id>` indexes the last `catalogue` this client read;
the server itself catches a stale one (`InstrumentChanged`, see `md_wire::grpc::RejectCode`)
and this client reports it rather than retrying, since a retry of the same request cannot help.

## The frame

An interactive terminal gets the book redrawn in place - the panel is rewritten on every
update rather than appended, so watching one instrument does not scroll the terminal into an
endless log:

```
#5 binance_spot:BTCUSDT bitstamp:btcusdt   spread 12.50000000

      68450.10000000       0.15000000  binance_spot
      68449.80000000       0.42000000  bitstamp
      68448.20000000       0.30000000  binance_spot
      68448.10000000       0.55000000  bitstamp
```

Asks and bids are not two blocks. The book is merged across venues, so an ask from one venue
can sit below a bid from another - the merge can cross - and the panel reflects that literally:
one column ordered by descending price, ask above bid on a tie, with the row's colour (red
ask, green bid) as the only thing saying which side a level came from. A crossed book needs no
special case; it simply reads as an ask row below a bid row, alongside a negative spread on the
header line. Both sides empty - the server's way of saying a venue is bootstrapping or
resyncing - prints the header with a `NaN` spread and a dim `no book` line.

The panel is a fixed height: the protocol's ten-level-per-side maximum, always, padded with
blank rows past whatever the server actually sent. A fixed height is what lets the prompt sit
below it without drifting, and that in turn is what lets this client redraw the panel with
nothing but hand-rolled ANSI escapes - save the cursor, step up to the panel's first row, clear
and rewrite each row, restore the cursor - rather than raw mode. The prompt line itself is never
touched, so the terminal's own echo of whatever is being typed survives a redraw arriving
mid-keystroke.

A non-interactive stdout (a pipe, a redirect) gets a different, plain-text rendering instead -
one appended, timestamped block per update - since there is no cursor for an escape sequence to
move. `cargo run -p md_client | cat` is how to get that even from a real terminal.

## Errors

A stream that ends - the server restarted, the connector went away, anything else - is reported
on a status line and the prompt returns; this client does not reconnect on its own. Re-run
`sub` (or `catalogue` first, if the catalogue itself might have changed) to try again.

## Library

Everything except argument parsing lives in the `md_client` library, so `md_server`'s
end-to-end test can drive the server with a real generated tonic client without linking a
binary crate. `render` holds every byte this client ever writes to a terminal - the line and
frame regimes both live there, so a test can pick either one without needing a real terminal -
and `command` is the REPL's command language, parsed independently of any I/O.
