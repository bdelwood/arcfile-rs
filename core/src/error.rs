use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArcError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("{0}")]
    Format(String),

    #[error("Unknown register: {0}")]
    RegMap(String),

    #[error("Arcfile corrupted: {0}")]
    Corrupted(String),

    #[error("Unknown format for file: 0x{0:04x}")]
    UnknownRegFlag(u32),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

pub type ArcResult<T> = Result<T, ArcError>;
