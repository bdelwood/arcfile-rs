use crate::regmap::{RegBlockSpec, RegType};

#[derive(Debug)]
pub struct RegData {
    data: RegValues,
    nsamp: usize,
    nchan: usize,
    channels: Option<Vec<usize>>,
}

impl RegData {
    pub fn new(spec: &RegBlockSpec, channels: Option<Vec<usize>>) -> Self {
        let nchan = channels.as_ref().map_or(spec.nchan, |ch| ch.len());
        Self {
            data: RegValues::empty(spec.typeword.reg_type),
            nsamp: 0,
            nchan,
            channels,
        }
    }

    pub fn nchan(&self) -> usize {
        self.nchan
    }

    pub fn nsamp(&self) -> usize {
        self.nsamp
    }

    pub fn data(self) -> RegValues {
        self.data
    }

    pub fn push_frame(&mut self, bytes: &[u8], spec: &RegBlockSpec) {
        match &self.channels {
            None => self.data.push_raw(bytes),
            Some(channels) => {
                let bpe = spec.element_size();
                let row_size = spec.nchan * bpe;
                for t in 0..spec.spf.max(1) {
                    for &ch in channels {
                        let start = t * row_size + ch * bpe;
                        self.data.push_raw(&bytes[start..start + bpe]);
                    }
                }
            }
        }
        self.nsamp += spec.spf.max(1);
    }
}

#[derive(Debug, Clone)]
pub enum RegValues {
    U8(Vec<u8>),
    I8(Vec<i8>),
    U16(Vec<u16>),
    I16(Vec<i16>),
    U32(Vec<u32>),
    I32(Vec<i32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Utc(Vec<[u32; 2]>),
}

impl RegValues {
    /// Create an empty typed container for a given register type.
    pub fn empty(rt: RegType) -> Self {
        match rt {
            RegType::UChar => Self::U8(Vec::new()),
            RegType::Char => Self::I8(Vec::new()),
            RegType::Bool => Self::Bool(Vec::new()),
            RegType::UShort => Self::U16(Vec::new()),
            RegType::Short => Self::I16(Vec::new()),
            RegType::UInt => Self::U32(Vec::new()),
            RegType::Int => Self::I32(Vec::new()),
            RegType::Float => Self::F32(Vec::new()),
            RegType::Double => Self::F64(Vec::new()),
            RegType::Utc => Self::Utc(Vec::new()),
        }
    }

    /// Append one frame's worth of raw bytes.
    pub fn push_raw(&mut self, bytes: &[u8]) {
        match self {
            Self::U8(v) => v.extend_from_slice(bytes),
            Self::I8(v) => v.extend(bytes.iter().map(|&b| b as i8)),
            Self::Bool(v) => v.extend(bytes.iter().map(|&b| b != 0)),
            Self::U16(v) => v.extend(
                bytes
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes(c.try_into().unwrap())),
            ),
            Self::I16(v) => v.extend(
                bytes
                    .chunks_exact(2)
                    .map(|c| i16::from_le_bytes(c.try_into().unwrap())),
            ),
            Self::U32(v) => v.extend(
                bytes
                    .chunks_exact(4)
                    .map(|c| u32::from_le_bytes(c.try_into().unwrap())),
            ),
            Self::I32(v) => v.extend(
                bytes
                    .chunks_exact(4)
                    .map(|c| i32::from_le_bytes(c.try_into().unwrap())),
            ),
            Self::F32(v) => v.extend(
                bytes
                    .chunks_exact(4)
                    .map(|c| f32::from_le_bytes(c.try_into().unwrap())),
            ),
            Self::F64(v) => v.extend(
                bytes
                    .chunks_exact(8)
                    .map(|c| f64::from_le_bytes(c.try_into().unwrap())),
            ),
            Self::Utc(v) => v.extend(bytes.chunks_exact(8).map(|c| {
                [
                    u32::from_le_bytes(c[..4].try_into().unwrap()),
                    u32::from_le_bytes(c[4..].try_into().unwrap()),
                ]
            })),
        }
    }
}
