use thiserror::Error;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Request timeout: {0}")]
    Timeout(String),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("Client error: {0}")]
    ClientError(String),

    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("Connection closed")]
    ConnectionClosed,
}

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Send failed after {attempts} attempts: {reason}")]
    SendFailed { attempts: usize, reason: String },

    #[error("Receive failed after {attempts} attempts: {reason}")]
    ReceiveFailed { attempts: usize, reason: String },

    #[error("Timeout after {duration_ms}ms: {operation}")]
    Timeout { duration_ms: u64, operation: String },

    #[error("Connection to '{name}' failed after {attempts} attempts: {reason}")]
    ConnectionFailed {
        name: String,
        attempts: usize,
        reason: String,
    },

    #[error("Not connected")]
    NotConnected,

    #[error("Invalid buffer state: {0}")]
    InvalidBufferState(String),

    #[error("Shared memory creation failed for '{name}': {reason}")]
    SharedMemoryCreation { name: String, reason: String },
}

pub type Result<T> = std::result::Result<T, RpcError>;
pub type TransportResult<T> = std::result::Result<T, TransportError>;

impl From<bincode::Error> for RpcError {
    fn from(err: bincode::Error) -> Self {
        RpcError::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(err: serde_json::Error) -> Self {
        RpcError::Serialization(err.to_string())
    }
}
