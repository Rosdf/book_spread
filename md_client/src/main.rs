//! An interactive client for the `md.v1.MarketData` book feed: fetch the catalogue, subscribe
//! to an instrument by its catalogue id, and watch its book update in place.
//!
//! ```text
//! cargo run -p md_client                                          # starts at the prompt
//! cargo run -p md_client -- --addr 127.0.0.1:50051 binance_spot BTCUSDT
//! ```
//!
//! At the prompt: `catalogue` lists what the server carries, `sub <id>` subscribes (replacing
//! any current subscription), `unsub` drops it, `help` repeats the command list, `quit` exits -
//! see [`md_client::command`]. An interactive terminal gets [`render::Format::Frame`]: the book
//! redrawn in place, one column of levels ordered by descending price since the book is merged
//! across venues and can cross, coloured red for an ask and green for a bid. A pipe gets
//! [`render::Format::Line`] instead - plain, appended text, since there is no cursor to move.
//! `--addr <addr>` names the server; without it, [`DEFAULT_ADDR`].

// The binary is a thin shell over the library, so these reach it only through `md_client`;
// `thiserror` is used by its `command::ParseCommandError` derive, which this target never
// names directly. Naming all three here is what keeps `unused_crate_dependencies` quiet for
// this target.
use thiserror as _;
use tonic as _;
use tonic_prost as _;

use md_client::command::{self, Command};
use md_client::render::{self, Format, Frame};
use md_client::{DEFAULT_ADDR, VenueNames, catalogue, follow, reject_code};
use md_proto::md::v1 as proto;
use md_wire::grpc::RejectCode;
use std::io::Write as _;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { addr, pair } = parse_args(std::env::args().skip(1))?;
    let format = Format::detect();

    let fetched = catalogue(&addr).await?;
    let venues = VenueNames::from_catalogue(&fetched);
    let (events_tx, mut events_rx) = mpsc::channel(4);
    let mut repl = Repl {
        addr,
        format,
        venues,
        catalogue: fetched,
        subscription: None,
        current_frame: None,
        frame_region_exists: false,
        events_tx,
    };

    if let Some((venue, symbol)) = pair {
        match find(&repl.catalogue, &venue, &symbol) {
            Some(instrument) => repl.do_sub(instrument.idx),
            None => println!("{}", not_carried(&repl.catalogue, &venue, &symbol)),
        }
    }
    print_prompt();

    let mut stdin_rx = spawn_stdin_reader();
    loop {
        tokio::select! {
            received = stdin_rx.recv() => {
                let Some(typed) = received else { break };
                if typed.trim().is_empty() {
                    print_prompt();
                    continue;
                }
                match command::parse(&typed) {
                    Ok(Command::Catalogue) => repl.do_catalogue().await,
                    Ok(Command::Sub(id)) => repl.do_sub(id),
                    Ok(Command::Unsub) => {
                        repl.stop_subscription();
                        print_prompt();
                    }
                    Ok(Command::Help) => repl.print_block(command::HELP),
                    Ok(Command::Quit) => {
                        repl.stop_subscription();
                        break;
                    }
                    Err(err) => repl.print_block(&err.to_string()),
                }
            }
            Some(event) = events_rx.recv() => repl.handle_stream_ended(event),
        }
    }
    Ok(())
}

/// One stream's end, as its subscription task reports it back to the REPL - the only thing
/// that task sends: every book update it renders straight to stdout itself.
type StreamEnded = Result<(), tonic::Status>;

/// State the REPL carries between prompts.
struct Repl {
    addr: Box<str>,
    format: Format,
    venues: VenueNames,
    catalogue: proto::CatalogueResponse,
    /// The task streaming the current subscription, if any - `abort()`ed by `unsub`, a
    /// replacing `sub`, `catalogue`, and `quit` alike.
    subscription: Option<tokio::task::JoinHandle<()>>,
    /// The active subscription's frame, when [`Format::Frame`] is in use - `None` both before
    /// the first subscribe and whenever nothing is currently subscribed.
    current_frame: Option<Frame>,
    /// Whether [`Frame::init`]'s blank lines have ever been printed. Printed once, the first
    /// time anything is subscribed under [`Format::Frame`] - later subscriptions reuse the same
    /// screen rows rather than scrolling a fresh block into existence, which is what would
    /// grow the scrollback on every `sub`.
    frame_region_exists: bool,
    events_tx: mpsc::Sender<StreamEnded>,
}

/// The prompt itself: not a `Repl` method, since it names no field of it.
fn print_prompt() {
    print!("> ");
    let _ = std::io::stdout().flush();
}

impl Repl {
    /// Clears the frame region in place, if one is currently drawn - leaving its rows blank on
    /// screen rather than removing them, since [`Frame::init`]'s newlines are only ever printed
    /// once.
    fn clear_frame(&mut self) {
        if self.current_frame.take().is_some() {
            print!("{}", render::redraw(&render::blank_rows()));
            let _ = std::io::stdout().flush();
        }
    }

    /// Prints `text` as its own block, below anything already on screen, then a fresh prompt.
    /// What every command other than `sub` answers with.
    fn print_block(&mut self, text: &str) {
        self.clear_frame();
        print!("\n{text}\n");
        let _ = std::io::stdout().flush();
        print_prompt();
    }

    /// Aborts the current subscription's task, if any, and clears its frame.
    fn stop_subscription(&mut self) {
        if let Some(task) = self.subscription.take() {
            task.abort();
        }
        self.clear_frame();
    }

    /// Re-fetches the catalogue - the server can be restarted under this client - and lists it.
    async fn do_catalogue(&mut self) {
        self.stop_subscription();
        match catalogue(&self.addr).await {
            Ok(fresh) => {
                self.venues = VenueNames::from_catalogue(&fresh);
                let listing = render::catalogue_listing(&fresh, &self.venues);
                self.catalogue = fresh;
                self.print_block(&listing);
            }
            Err(status) => self.print_block(&format!("could not fetch the catalogue: {status}")),
        }
    }

    /// Subscribes to the instrument at `id` in the last catalogue read, replacing any current
    /// subscription. `id` indexes that listing, so a stale one - the server restarted with a
    /// different catalogue file since it was read - is caught by the server as
    /// `InstrumentChanged`, not here.
    fn do_sub(&mut self, id: u32) {
        self.stop_subscription();

        let Some(instrument) = self
            .catalogue
            .instruments
            .iter()
            .find(|entry| entry.idx == id)
        else {
            self.print_block(&format!("no instrument #{id} here - try \"catalogue\""));
            return;
        };

        // Echoed back to the server exactly as the catalogue listed them - required, see
        // `SubscribeBookRequest` in `market_data.proto`.
        let pairs: Box<[proto::SubscribePair]> = instrument
            .pairs
            .iter()
            .map(|pair| proto::SubscribePair {
                venue: self.venues.name(pair.venue_idx).into(),
                symbol: pair.symbol.clone(),
            })
            .collect();
        let label = render::instrument_label(instrument.idx, &instrument.pairs, &self.venues);
        let instrument_idx = instrument.idx;

        match self.format {
            Format::Frame => {
                if !self.frame_region_exists {
                    print!("{}", Frame::init());
                    print_prompt();
                    self.frame_region_exists = true;
                }
                self.current_frame = Some(Frame::new(label.clone()));
            }
            Format::Line => self.print_block(&format!("subscribed to {label}")),
        }

        let addr = self.addr.clone();
        let venues = self.venues.clone();
        let events_tx = self.events_tx.clone();
        let format = self.format;
        let subscribed_frame = self.current_frame.clone();
        let task = tokio::spawn(async move {
            let mut on_update = |book: &proto::BookUpdate| match format {
                Format::Frame => {
                    if let Some(frame) = &subscribed_frame {
                        print!("{}", render::redraw(&frame.render(book, &venues)));
                        let _ = std::io::stdout().flush();
                    }
                }
                Format::Line => println!("{}", render::line(book, &venues, &label)),
            };
            let result = follow(&addr, instrument_idx, pairs, &mut on_update).await;
            let _ = events_tx.send(result).await;
        });
        self.subscription = Some(task);
    }

    /// What a subscription task reports once its stream ends - no reconnect, so this is always
    /// the end of that subscription. The frame it drew is left on screen exactly as it last
    /// rendered, and the status is printed under it.
    fn handle_stream_ended(&mut self, result: StreamEnded) {
        self.subscription = None;
        self.current_frame = None;
        self.print_block(&stream_ended_message(result));
    }
}

/// What to say about a stream that ended: the canonical status says what kind of problem it
/// was, and the metadata, when the server sent it, says exactly which one and therefore whether
/// trying the same subscribe again could ever work.
fn stream_ended_message(result: StreamEnded) -> String {
    let Err(status) = result else {
        return "stream ended".to_owned();
    };
    match reject_code(&status) {
        Some(code) if code.retryable() => {
            format!(
                "stream ended: {} ({code:?}, worth retrying)",
                status.message()
            )
        }
        Some(RejectCode::InstrumentChanged) => format!(
            "stream ended: {} (InstrumentChanged) - the catalogue changed since it was read; \
             run \"catalogue\" and sub again",
            status.message()
        ),
        Some(code) => format!("stream ended: {} ({code:?})", status.message()),
        None => format!("stream ended: {status}"),
    }
}

/// Reads lines from stdin on a blocking thread - stdin is line-buffered and blocking, so this
/// is what lets the REPL loop await it alongside a subscription's events instead of stalling on
/// it.
fn spawn_stdin_reader() -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel(16);
    tokio::task::spawn_blocking(move || {
        use std::io::BufRead as _;
        for read in std::io::stdin().lock().lines() {
            let Ok(typed) = read else { break };
            if tx.blocking_send(typed).is_err() {
                break;
            }
        }
    });
    rx
}

/// What the command line asked for: which server, and which instrument (if any) to be already
/// subscribed to before the first prompt.
struct Args {
    addr: Box<str>,
    pair: Option<(Box<str>, Box<str>)>,
}

/// Parses argv, already past the program name: an optional `--addr <addr>` naming the server -
/// [`DEFAULT_ADDR`] otherwise - and an optional `<venue> <symbol>` pair, in either order
/// relative to `--addr`.
fn parse_args(argv: impl IntoIterator<Item = String>) -> anyhow::Result<Args> {
    let mut addr = None;
    let mut positional = Vec::new();

    let mut words = argv.into_iter();
    while let Some(word) = words.next() {
        if word == "--addr" {
            addr = Some(
                words
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--addr needs a value"))?,
            );
        } else {
            positional.push(word);
        }
    }

    let pair = match positional.as_slice() {
        [] => None,
        [venue, symbol] => Some((Box::from(venue.as_str()), Box::from(symbol.as_str()))),
        _ => anyhow::bail!("usage: md_client [--addr <addr>] [<venue> <symbol>]"),
    };
    Ok(Args {
        addr: addr.map_or_else(|| Box::from(DEFAULT_ADDR), String::into_boxed_str),
        pair,
    })
}

/// The instrument carrying `(venue, symbol)`, if this server carries one.
///
/// The venue name is matched case-insensitively - it is this build's own spelling of a venue -
/// while the symbol is not: a venue's symbol is whatever that venue calls it, and the
/// catalogue carries it verbatim.
fn find<'a>(
    catalogue: &'a proto::CatalogueResponse,
    venue: &str,
    symbol: &str,
) -> Option<&'a proto::InstrumentEntry> {
    let venue_idx = catalogue
        .venues
        .iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(venue))?
        .idx;

    catalogue.instruments.iter().find(|instrument| {
        instrument
            .pairs
            .iter()
            .any(|pair| pair.venue_idx == venue_idx && pair.symbol == symbol)
    })
}

/// What to print when the server does not carry what the command line asked for: the pair, and
/// everything it does carry - the only way a user finds out what to type instead.
fn not_carried(catalogue: &proto::CatalogueResponse, venue: &str, symbol: &str) -> String {
    let venues = VenueNames::from_catalogue(catalogue);
    format!(
        "this server does not carry {venue}/{symbol}. It carries:\n{}",
        render::catalogue_listing(catalogue, &venues)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|&word| word.to_owned()).collect()
    }

    #[test]
    fn no_args_is_the_default_addr_and_no_pair() {
        let parsed = parse_args(args(&[])).unwrap();
        assert_eq!(parsed.addr.as_ref(), DEFAULT_ADDR);
        assert_eq!(parsed.pair, None);
    }

    #[test]
    fn addr_alone_is_taken_and_the_default_is_not_used() {
        let parsed = parse_args(args(&["--addr", "example.com:1"])).unwrap();
        assert_eq!(parsed.addr.as_ref(), "example.com:1");
        assert_eq!(parsed.pair, None);
    }

    #[test]
    fn addr_and_pair_work_in_either_order() {
        let before = parse_args(args(&[
            "--addr",
            "example.com:1",
            "binance_spot",
            "BTCUSDT",
        ]))
        .unwrap();
        assert_eq!(before.addr.as_ref(), "example.com:1");
        assert_eq!(
            before.pair,
            Some((Box::from("binance_spot"), Box::from("BTCUSDT")))
        );

        let after = parse_args(args(&[
            "binance_spot",
            "BTCUSDT",
            "--addr",
            "example.com:1",
        ]))
        .unwrap();
        assert_eq!(after.addr.as_ref(), "example.com:1");
        assert_eq!(
            after.pair,
            Some((Box::from("binance_spot"), Box::from("BTCUSDT")))
        );
    }

    #[test]
    fn addr_without_a_value_is_an_error() {
        assert!(parse_args(args(&["--addr"])).is_err());
    }

    #[test]
    fn a_lone_venue_or_a_third_positional_is_an_error() {
        assert!(parse_args(args(&["binance_spot"])).is_err());
        assert!(parse_args(args(&["binance_spot", "BTCUSDT", "extra"])).is_err());
    }
}
