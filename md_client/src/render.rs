//! Every byte the client writes to a terminal, in one place.
//!
//! Two regimes, chosen once at startup by [`Format::detect`]: [`Format::Line`] appends a
//! timestamped block per update - what a piped run or a script parses - and [`Format::Frame`]
//! redraws a fixed-height panel in place, for an interactive terminal. Keeping both here rather
//! than scattering `println!`s through `main.rs` is what lets a test pick either one without a
//! real terminal.

use md_proto::md::v1 as proto;
use std::fmt::Write as _;
use std::io::IsTerminal as _;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::VenueNames;

/// The two ways a [`proto::BookUpdate`] gets printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// One appended, timestamped block per update - the pre-rework behaviour, kept for
    /// anything scripted against it.
    Line,
    /// A fixed-height panel, redrawn in place. Needs a real terminal: the redraw escapes move
    /// the cursor by lines the shell itself put on screen, which a pipe has no notion of.
    Frame,
}

impl Format {
    /// [`Format::Frame`] for an interactive terminal, [`Format::Line`] otherwise - a pipe or a
    /// redirect gets clean text rather than escape sequences it cannot interpret.
    #[must_use]
    pub fn detect() -> Self {
        if std::io::stdout().is_terminal() {
            Self::Frame
        } else {
            Self::Line
        }
    }
}

/// One line of output, stamped with the local microsecond it was received - so a run of these
/// is something latency can be measured out of. [`Format::Line`]'s renderer.
#[must_use]
pub fn line(book: &proto::BookUpdate, venues: &VenueNames, label: &str) -> Box<str> {
    let at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();

    let Some((bid, ask)) = book.bids.first().zip(book.asks.first()) else {
        // Both sides empty is the connector saying it has no book - bootstrapping, or
        // resyncing. Whatever was on screen a moment ago is not the market any more. No
        // level is available to name a venue, so the requested instrument stands in for it.
        return format!("{at} {label:<24} no book").into_boxed_str();
    };
    format!(
        "{at} spread {:.8}\n  bid {:<13} {:>14.8} x {:<12.8}\n  ask {:<13} {:>14.8} x {:<12.8}",
        book.spread,
        venues.name(bid.venue_idx),
        bid.price,
        bid.size,
        venues.name(ask.venue_idx),
        ask.price,
        ask.size
    )
    .into_boxed_str()
}

/// The id-and-pairs label an instrument is known by everywhere in this client: the catalogue
/// listing, and a [`Frame`]'s header.
#[must_use]
pub fn instrument_label(idx: u32, pairs: &[proto::Pair], venues: &VenueNames) -> Box<str> {
    let mut label = format!("#{idx}");
    for pair in pairs {
        let _ = write!(label, " {}:{}", venues.name(pair.venue_idx), pair.symbol);
    }
    label.into_boxed_str()
}

/// The `catalogue` command's output: one line per instrument, id and every pair it carries.
///
/// The only way a user finds an id to `sub` on, so this is also what a missing pair is shown
/// against.
#[must_use]
pub fn catalogue_listing(catalogue: &proto::CatalogueResponse, venues: &VenueNames) -> Box<str> {
    let mut listing = String::new();
    for instrument in &catalogue.instruments {
        if !listing.is_empty() {
            listing.push('\n');
        }
        listing.push_str(&instrument_label(instrument.idx, &instrument.pairs, venues));
    }
    listing.into_boxed_str()
}

/// Levels the server sends per side, per [`proto::BookUpdate`]'s own doc comment.
const MAX_LEVELS: usize = 10;

const SAVE_CURSOR: &str = "\x1b7";
const RESTORE_CURSOR: &str = "\x1b8";
const CLEAR_LINE: &str = "\x1b[2K";
const DOWN_ONE: &str = "\x1b[1B";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

/// Which side of the book a merged row came from - the only thing telling one apart once both
/// sides share a single, descending-by-price column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Ask,
    Bid,
}

/// Interleaves both sides into one descending-by-price column.
///
/// The server sends each side best-first: asks ascending, bids descending. Reversing the asks
/// makes both descending, and a stable two-way merge on descending price does the rest - taking
/// the ask on a tie, so an uncrossed book still reads asks-then-bids. A merged book can cross
/// (an ask below the best bid), and this needs no case for it: a crossed pair simply comes out
/// interleaved rather than stacked, which is exactly what lets the row colours show it.
fn merged_rows(book: &proto::BookUpdate) -> Vec<(Side, &proto::Level)> {
    let mut asks = book.asks.iter().rev().peekable();
    let mut bids = book.bids.iter().peekable();
    let mut rows = Vec::with_capacity(book.asks.len() + book.bids.len());
    loop {
        // `peek` borrows the iterator, so the choice of side is made and that borrow dropped
        // before either iterator is advanced - `next` needs it back.
        let take_ask = match (asks.peek(), bids.peek()) {
            (Some(ask), Some(bid)) => ask.price >= bid.price,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_ask {
            rows.extend(asks.next().map(|ask| (Side::Ask, ask)));
        } else {
            rows.extend(bids.next().map(|bid| (Side::Bid, bid)));
        }
    }
    rows
}

/// One row's text: price, size, venue - numbers right-aligned, venue left-aligned - wrapped in
/// the colour that is this row's only sign of which side it came from.
fn row_text(colour: &str, price: f64, size: f64, venue: &str) -> Box<str> {
    format!("{colour}{price:>14.8}  {size:>12.8}  {venue:<12}{RESET}").into_boxed_str()
}

/// The in-place panel for one instrument: a header line built once at subscribe time, redrawn
/// against every [`proto::BookUpdate`] that arrives for it.
#[derive(Debug, Clone)]
pub struct Frame {
    header: Box<str>,
}

impl Frame {
    /// Rows the frame occupies: the header, a blank separator, and the protocol's per-side
    /// maximum on each side - the protocol max rather than the levels a given update happens
    /// to carry, so the region's height never changes and the prompt below it never drifts.
    pub const ROWS: usize = 2 + 2 * MAX_LEVELS;

    /// `label` is the header text: an id and every pair the subscribed instrument carries, see
    /// [`instrument_label`].
    #[must_use]
    pub fn new(label: Box<str>) -> Self {
        Self { header: label }
    }

    /// Blank lines to print once, before the first draw, so the frame's region exists on
    /// screen for [`redraw`] to move the cursor into.
    #[must_use]
    pub fn init() -> String {
        "\n".repeat(Self::ROWS)
    }

    /// The frame's content for one [`proto::BookUpdate`]: exactly [`Frame::ROWS`] rows, padding
    /// with blanks past whatever levels the server actually sent.
    ///
    /// Plain strings, not yet wrapped in the redraw escapes - [`redraw`] does that - so a test
    /// can assert on row content without decoding ANSI.
    #[must_use]
    pub fn render(&self, book: &proto::BookUpdate, venues: &VenueNames) -> Box<[Box<str>]> {
        let mut rows = Vec::with_capacity(Self::ROWS);
        rows.push(format!("{} spread {:.8}", self.header, book.spread).into_boxed_str());
        rows.push(Box::from(""));

        if book.asks.is_empty() && book.bids.is_empty() {
            // Both sides empty is the server's resync signal, not a book with nothing quoted
            // in it - said outright rather than left as a blank panel.
            rows.push(format!("{DIM}no book{RESET}").into_boxed_str());
        } else {
            for (side, level) in merged_rows(book) {
                let colour = match side {
                    Side::Ask => RED,
                    Side::Bid => GREEN,
                };
                rows.push(row_text(
                    colour,
                    level.price,
                    level.size,
                    &venues.name(level.venue_idx),
                ));
            }
        }

        rows.resize(Self::ROWS, Box::from(""));
        rows.into_boxed_slice()
    }
}

/// [`Frame::ROWS`] blank rows - what unsubscribing or switching to the catalogue listing
/// redraws over a frame with, via [`redraw`].
#[must_use]
pub fn blank_rows() -> Vec<Box<str>> {
    vec![Box::from(""); Frame::ROWS]
}

/// Wraps `rows` in the escapes that redraw them in place, leaving the prompt line untouched.
///
/// Cursor starts on the prompt line. Save it, move up `rows.len()` lines to the frame's first
/// row, then walk down one row at a time, on each returning to column 0, clearing the line and
/// writing it - so every row is written from column 0 regardless of where the cursor happened
/// to be. That walks the cursor back down to exactly where it started, but the saved position
/// is restored anyway rather than relied on, since it is also what puts the cursor back at the
/// right column within a line the user was mid-typing. No raw mode is needed: the prompt line
/// itself is never touched, so the terminal's own echo of what the user has typed survives.
#[must_use]
pub fn redraw(rows: &[Box<str>]) -> String {
    let mut out = String::from(SAVE_CURSOR);
    let _ = write!(out, "\x1b[{}A", rows.len());
    for row in rows {
        out.push('\r');
        out.push_str(CLEAR_LINE);
        out.push_str(row);
        out.push_str(DOWN_ONE);
    }
    out.push_str(RESTORE_CURSOR);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: f64, size: f64, venue_idx: u32) -> proto::Level {
        proto::Level {
            price,
            size,
            venue_idx,
        }
    }

    fn venues() -> VenueNames {
        let catalogue = proto::CatalogueResponse {
            venues: vec![
                proto::VenueEntry {
                    idx: 0,
                    name: "binance_spot".to_owned(),
                },
                proto::VenueEntry {
                    idx: 1,
                    name: "bitstamp".to_owned(),
                },
            ],
            instruments: vec![],
        };
        VenueNames::from_catalogue(&catalogue)
    }

    #[test]
    fn merges_both_sides_into_one_descending_column() {
        let book = proto::BookUpdate {
            asks: vec![level(101.0, 1.0, 0), level(102.0, 1.0, 0)],
            bids: vec![level(100.0, 1.0, 1), level(99.0, 1.0, 1)],
            spread: 1.0,
        };
        let rows = merged_rows(&book);
        let prices: Vec<f64> = rows.iter().map(|(_, level)| level.price).collect();
        assert_eq!(prices, [102.0, 101.0, 100.0, 99.0]);
        assert_eq!(rows[0].0, Side::Ask);
        assert_eq!(rows[1].0, Side::Ask);
        assert_eq!(rows[2].0, Side::Bid);
        assert_eq!(rows[3].0, Side::Bid);
    }

    #[test]
    fn a_crossed_book_still_comes_out_monotonic_with_the_ask_first_on_a_tie() {
        // The ask sits below the bid - a crossed book - and the two meet at one shared price.
        let book = proto::BookUpdate {
            asks: vec![level(99.0, 1.0, 0)],
            bids: vec![level(100.0, 1.0, 1), level(99.0, 1.0, 1)],
            spread: -1.0,
        };
        let rows = merged_rows(&book);
        let prices: Vec<f64> = rows.iter().map(|(_, level)| level.price).collect();
        assert_eq!(prices, [100.0, 99.0, 99.0]);
        assert_eq!(rows[0].0, Side::Bid);
        // The tied pair: ask goes above the bid at the same price.
        assert_eq!(rows[1].0, Side::Ask);
        assert_eq!(rows[2].0, Side::Bid);
    }

    #[test]
    fn frame_pads_to_its_fixed_row_count() {
        let frame = Frame::new(Box::from("#0 binance_spot:BTCUSDT"));
        let book = proto::BookUpdate {
            asks: vec![level(101.0, 1.0, 0)],
            bids: vec![level(100.0, 1.0, 0)],
            spread: 1.0,
        };
        let rows = frame.render(&book, &venues());
        assert_eq!(rows.len(), Frame::ROWS);
        assert!(rows[0].contains("#0 binance_spot:BTCUSDT"));
        assert!(rows[0].contains("spread 1.00000000"));
        assert_eq!(rows[1].as_ref(), "");
        assert!(rows[2].contains("binance_spot"));
        // Padding past the two levels sent is blank.
        assert_eq!(rows[4].as_ref(), "");
    }

    #[test]
    fn empty_book_renders_the_dim_no_book_line() {
        let frame = Frame::new(Box::from("#0 binance_spot:BTCUSDT"));
        let book = proto::BookUpdate {
            asks: vec![],
            bids: vec![],
            spread: f64::NAN,
        };
        let rows = frame.render(&book, &venues());
        assert_eq!(rows.len(), Frame::ROWS);
        assert!(rows[0].contains("spread NaN"));
        assert!(rows[2].contains("no book"));
    }
}
