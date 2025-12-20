//! Layer 2: Channel - typed communication with serialization.

pub mod message;
pub mod serde;
pub mod typed;

pub use message::{MessageChannel, MessageChannelAdapter};
pub use serde::SerdeChannel;
pub use typed::TypedChannel;

// Re-export for convenience
pub use crate::codec::{BincodeCodec, Codec};
