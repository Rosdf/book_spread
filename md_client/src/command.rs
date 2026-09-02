//! The REPL's command language.
//!
//! One line in, one [`Command`] out (or the one way that can fail) - kept separate from
//! `main.rs` so it is unit-testable without a terminal or a server.

/// What a line at the prompt asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Fetch the catalogue (again) and print the listing.
    Catalogue,
    /// Subscribe to the instrument at this catalogue id, replacing any current subscription.
    Sub(u32),
    /// Drop the current subscription, if any.
    Unsub,
    /// Print the command list.
    Help,
    /// Leave the REPL.
    Quit,
}

/// The command text, verbatim, when it named no [`Command`].
///
/// The one way `parse` fails, so it carries what could not be made sense of rather than a
/// generic message - a REPL's whole feedback loop is echoing back what it didn't understand.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unrecognized command: {0:?} (try \"help\")")]
pub struct ParseCommandError(pub Box<str>);

/// The command set, as `help` prints it.
pub const HELP: &str = "\
commands:
  catalogue, c        fetch and list what the server carries
  sub <id>, s <id>    subscribe to the instrument at <id>, replacing any current subscription
  unsub, u            drop the current subscription
  help                print this text
  quit, q             exit";

/// Parses one line typed at the prompt into a [`Command`].
///
/// Blank input and surrounding whitespace are not commands worth failing over - a blank line
/// parses to nothing meaningful for the caller to act on, so this returns a `Command` only for
/// non-blank input; the caller is expected to skip blank lines itself. Matching is
/// case-insensitive: a command is punctuation to this REPL, not something worth typing exactly.
///
/// # Errors
///
/// [`ParseCommandError`] when the line names no known command, or `sub` names something that
/// is not an id.
pub fn parse(line: &str) -> Result<Command, ParseCommandError> {
    let mut words = line.split_whitespace();
    let Some(word) = words.next() else {
        // A blank line is handled by the caller; asked to parse one anyway, it names nothing.
        return Err(ParseCommandError(line.into()));
    };

    match word.to_ascii_lowercase().as_str() {
        "catalogue" | "c" => Ok(Command::Catalogue),
        "unsub" | "u" => Ok(Command::Unsub),
        "help" => Ok(Command::Help),
        "quit" | "q" => Ok(Command::Quit),
        "sub" | "s" => {
            let id = words.next().ok_or_else(|| ParseCommandError(line.into()))?;
            id.parse::<u32>()
                .map(Command::Sub)
                .map_err(|_| ParseCommandError(line.into()))
        }
        _ => Err(ParseCommandError(line.into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_every_command_and_its_alias() {
        assert_eq!(parse("catalogue"), Ok(Command::Catalogue));
        assert_eq!(parse("c"), Ok(Command::Catalogue));
        assert_eq!(parse("unsub"), Ok(Command::Unsub));
        assert_eq!(parse("u"), Ok(Command::Unsub));
        assert_eq!(parse("help"), Ok(Command::Help));
        assert_eq!(parse("quit"), Ok(Command::Quit));
        assert_eq!(parse("q"), Ok(Command::Quit));
        assert_eq!(parse("sub 3"), Ok(Command::Sub(3)));
        assert_eq!(parse("s 3"), Ok(Command::Sub(3)));
    }

    #[test]
    fn is_case_insensitive_and_tolerates_surrounding_whitespace() {
        assert_eq!(parse("  SUB   7  "), Ok(Command::Sub(7)));
        assert_eq!(parse("QUIT"), Ok(Command::Quit));
    }

    #[test]
    fn rejects_sub_without_an_id_or_with_a_non_numeric_one() {
        assert!(parse("sub").is_err());
        assert!(parse("sub abc").is_err());
    }

    #[test]
    fn rejects_unknown_words() {
        assert!(parse("subscribe 3").is_err());
        assert!(parse("").is_err());
    }
}
