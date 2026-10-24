use super::AlignedSbeSchema;
use super::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateSchema, SCHEMA_ID, SCHEMA_VERSION, SnapshotSchema,
};

pub struct OrderbookSchema;

impl AlignedSbeSchema for OrderbookSchema {
    const SCHEMA_ID: u16 = SCHEMA_ID;
    const SCHEMA_VERSION: u16 = SCHEMA_VERSION;

    type Message<'buf> = AnyMessage<'buf>;

    fn try_decode<'buf>(buffer: &'buf [u8], offset: usize) -> anyhow::Result<Self::Message<'buf>> {
        Ok(AnyMessage::try_decode(buffer, offset)?)
    }

    fn encoded_length(message: &Self::Message<'_>) -> anyhow::Result<usize> {
        Ok(message.encoded_length_with_header()?)
    }

    fn expected_block_length(message: &Self::Message<'_>) -> anyhow::Result<usize> {
        Ok(match message {
            AnyMessage::Snapshot(_) => SnapshotSchema::BLOCK_LENGTH,
            AnyMessage::PriceLevelUpdate(_) => PriceLevelUpdateSchema::BLOCK_LENGTH,
            _ => anyhow::bail!("unknown message"),
        })
    }
}
