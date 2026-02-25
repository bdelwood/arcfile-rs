use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArcError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Unknown format for file: {0}")]
    Format(String),

    #[error("Memory allocation failure.")]
    NoMem,

    #[error("Unexpected end of file.")]
    UnexpectedEof,

    #[error("Unknown register: {0}")]
    RegMap(String),

    #[error("Arcfile corrupted: {0}")]
    Corrupted(String),

    #[error("Unknown format for file: 0x{0:04x}")]
    UnknownRegFlag(u32),
}

pub type ArcResult<T> = Result<T, ArcError>;
