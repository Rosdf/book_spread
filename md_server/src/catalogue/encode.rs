//! Turns a [`Catalogue`] into the `GetCatalogue` response body, once.
//!
//! Hand-encoded like `broadcast::book_encoder`'s books, for one shared reason rather than two: the
//! bytes are produced once at startup and handed to every caller as a `Bytes` clone, so
//! nothing here is on a hot path - what it buys is that the response never needs an
//! intermediate `CatalogueResponse` to exist, and that the encoding of a `uint32` field lives
//! in exactly one place in this crate. [`test::the_hand_encoder_agrees_with_prost`] is what
//! keeps it honest.
//!
//! Instruments are written in index order, which is simply [`crate::catalogue::Instruments`]'s
//! own order: it is a slice indexed by position, so there is no iteration order to defend
//! against the way a hash map would need.

use crate::catalogue::Catalogue;
use crate::encode::{put_message, put_string, put_uint32, put_venue_idx};
use bytes::{BufMut as _, Bytes, BytesMut};
use core_lib::Venue;
use md_wire::grpc::{MESSAGE_PREFIX, put_message_prefix};

/// `CatalogueResponse.venues`, field 1, wire type 2.
const VENUES_FIELD: u32 = 1;
/// `CatalogueResponse.instruments`, field 2, wire type 2.
const INSTRUMENTS_FIELD: u32 = 2;
/// `VenueEntry.idx` and `InstrumentEntry.idx`, field 1, and `Pair.venue_idx`, field 1.
const IDX_FIELD: u32 = 1;
/// `VenueEntry.name`, field 2.
const NAME_FIELD: u32 = 2;
/// `InstrumentEntry.pairs`, field 2, wire type 2.
const PAIRS_FIELD: u32 = 2;
/// `Pair.symbol`, field 2.
const SYMBOL_FIELD: u32 = 2;

/// The whole gRPC length-prefixed `CatalogueResponse` message, ready to be sent as one DATA
/// frame.
pub(crate) fn encode(catalogue: &Catalogue) -> Bytes {
    let mut message = BytesMut::new();
    message.put_bytes(0, MESSAGE_PREFIX);

    let mut entries = Vec::new();

    for venue in Venue::ALL {
        entries.clear();
        put_venue_idx(&mut entries, IDX_FIELD, venue);
        put_string(&mut entries, NAME_FIELD, venue.as_str());
        put_message(&mut message, VENUES_FIELD, &entries);
    }

    for (idx, pairs) in catalogue.instruments().iter() {
        entries.clear();
        put_uint32(&mut entries, IDX_FIELD, idx.get());
        for pair in pairs {
            let mut encoded = Vec::new();
            put_venue_idx(&mut encoded, IDX_FIELD, pair.venue());
            put_string(&mut encoded, SYMBOL_FIELD, pair.symbol());
            put_message(&mut entries, PAIRS_FIELD, &encoded);
        }
        put_message(&mut message, INSTRUMENTS_FIELD, &entries);
    }

    let len = u32::try_from(message.len() - MESSAGE_PREFIX).expect("a catalogue is far smaller than 4 GiB");
    put_message_prefix(&mut message[..MESSAGE_PREFIX], len);
    message.freeze()
}

#[cfg(test)]
mod test {
    use crate::catalogue::Catalogue;
    use core_lib::Venue;
    use md_proto::md::v1 as proto;
    use md_wire::grpc::{MESSAGE_PREFIX, message_len};
    use prost::Message as _;

    /// The same guard `broadcast::book_encoder` has: this is a second implementation of an
    /// encoding prost already implements, so the bytes have to be prost's - including the two
    /// places proto3 elides a default here, instrument index zero and an empty symbol. A venue
    /// index is never one of them: `crate::encode::venue_idx` numbers from one.
    #[test]
    fn the_hand_encoder_agrees_with_prost() {
        let catalogue = Catalogue::for_test(&[
            (0, &[(Venue::BinanceSpot, "BTCUSDT")]),
            (
                9,
                &[(Venue::BinanceSpot, "ETHUSDT"), (Venue::Bitstamp, "ethusd")],
            ),
            // An entry with no pairs at all, and one whose symbol is the empty string: both
            // are elision cases, and both are reachable from a hand-written catalogue file.
            (11, &[]),
            (12, &[(Venue::Bitstamp, "")]),
        ]);

        let expected = proto::CatalogueResponse {
            venues: vec![
                proto::VenueEntry {
                    idx: 1,
                    name: "binance_spot".to_owned(),
                },
                proto::VenueEntry {
                    idx: 2,
                    name: "bitstamp".to_owned(),
                },
            ],
            instruments: vec![
                proto::InstrumentEntry {
                    idx: 0,
                    pairs: vec![proto::Pair {
                        venue_idx: 1,
                        symbol: "BTCUSDT".to_owned(),
                    }],
                },
                proto::InstrumentEntry {
                    idx: 9,
                    pairs: vec![
                        proto::Pair {
                            venue_idx: 1,
                            symbol: "ETHUSDT".to_owned(),
                        },
                        proto::Pair {
                            venue_idx: 2,
                            symbol: "ethusd".to_owned(),
                        },
                    ],
                },
                proto::InstrumentEntry {
                    idx: 11,
                    pairs: Vec::new(),
                },
                proto::InstrumentEntry {
                    idx: 12,
                    pairs: vec![proto::Pair {
                        venue_idx: 2,
                        symbol: String::new(),
                    }],
                },
            ],
        }
        .encode_to_vec();

        let encoded = super::encode(&catalogue);
        let header = encoded[..MESSAGE_PREFIX]
            .try_into()
            .expect("the header is five bytes");
        assert_eq!(
            message_len(&header),
            Some(encoded.len() - MESSAGE_PREFIX),
            "the gRPC header must be uncompressed and describe the body that follows it"
        );
        assert_eq!(
            &encoded[MESSAGE_PREFIX..],
            expected.as_slice(),
            "the hand encoder must produce prost's bytes"
        );
    }

    /// A client decodes this with a generated codec, so what matters in the end is that the
    /// round trip lands the indices it will subscribe by.
    #[test]
    fn a_client_decodes_the_indices_it_will_subscribe_by() {
        let catalogue = Catalogue::for_test(&[(4, &[(Venue::Bitstamp, "btcusd")])]);
        let encoded = super::encode(&catalogue);

        let decoded = proto::CatalogueResponse::decode(&encoded[MESSAGE_PREFIX..])
            .expect("the message is a CatalogueResponse");
        assert_eq!(decoded.instruments.len(), 1);
        assert_eq!(decoded.instruments[0].idx, 4);
        assert_eq!(decoded.instruments[0].pairs[0].symbol, "btcusd");
        let venue_idx = decoded.instruments[0].pairs[0].venue_idx;
        assert_eq!(
            decoded
                .venues
                .iter()
                .find(|entry| entry.idx == venue_idx)
                .map(|entry| entry.name.as_str()),
            Some("bitstamp"),
            "a level's venue idx is only meaningful through the venue table"
        );
    }
}
