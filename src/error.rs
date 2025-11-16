use thiserror::Error;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),
}

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("Protocol error: {0}")]
    Protocol(String),
}

pub type Result<T> = std::result::Result<T, RpcError>;
pub type TransportResult<T> = std::result::Result<T, TransportError>;

impl From<bincode::Error> for RpcError {
    fn from(err: bincode::Error) -> Self {
        RpcError::Serialization(err.to_string())
    }
}
