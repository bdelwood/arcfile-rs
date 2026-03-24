use crate::regmap::{RegBlockSpec, RegType};

/// Register data is generic over storage state.
///
/// `RegData<Buffer>`: a pre-allocated typed buffer
/// that frames are scattered into during the read loop.
///
/// `RegData<RegValues>`: finalized typed column-major data.
/// Created by `finish()` after all frames have been read.
#[derive(Debug)]
pub struct RegData<S> {
    pub nchan: usize,
    pub nsamp: usize,
    pub(crate) reg_type: RegType,
    pub(crate) storage: S,
}

// essentially the on-disk format, loaded into memory
#[derive(Debug)]
pub(crate) struct Buffer {
    data: RegValues,
    spf: usize,
    elem_size: usize,
    reg_ofs: usize,
    channels: Vec<usize>,
    nframes_written: usize,
    nframes_capacity: usize,
}

impl RegData<Buffer> {
    /// Pre-allocate an output buffer for `nframes_est` frames.
    /// For compressed files this may be an overestimate, which `finish()` trims.
    /// If underestimated, `scatter_frame` grows by 2x as needed.
    pub(crate) fn new(
        spec: &RegBlockSpec,
        channels: Option<Vec<usize>>,
        nframes_est: usize,
    ) -> Self {
        let channels = channels.unwrap_or_else(|| (0..spec.nchan).collect());
        let nchan = channels.len();
        let spf = spec.spf.max(1);
        let elem_size = spec.element_size();
        let reg_type = spec.typeword.reg_type;

        Self {
            nchan,
            nsamp: 0,
            reg_type,
            storage: Buffer {
                data: RegValues::zeroed(reg_type, nframes_est * spf * nchan.max(1)),
                spf,
                elem_size,
                reg_ofs: spec.ofs,
                channels,
                nframes_written: 0,
                nframes_capacity: nframes_est,
            },
        }
    }

    /// Scatter one frame's register data into the output buffer.
    /// Grows the buffer if needed.
    #[inline]
    pub(crate) fn scatter_frame(&mut self, frame: &[u8]) {
        if self.storage.nframes_written >= self.storage.nframes_capacity {
            self.grow();
        }

        let buffer = &mut self.storage;
        let elem = buffer.spf * buffer.elem_size;
        let chan_stride = buffer.nframes_capacity * elem;
        let offset = buffer.nframes_written * elem;

        // Output layout is column-major: channels are outer, samples inner.
        // [ch0_s0, ch0_s1, ..., ch1_s0, ch1_s1, ...]
        // Selected channels may be non-contiguous in the frame,
        // so we index into the frame by channel and copy each one
        // into its contiguous column in the output buffer.
        let out = buffer.data.as_bytes_mut();

        for (col, &ch) in buffer.channels.iter().enumerate() {
            let src = buffer.reg_ofs + ch * elem;
            let dst = col * chan_stride + offset;
            out[dst..dst + elem].copy_from_slice(&frame[src..src + elem]);
        }

        buffer.nframes_written += 1;
        self.nsamp += buffer.spf;
    }

    /// Double the buffer capacity. Called by scatter_frame when full.
    /// Should be an edge csase, but included for robustness
    /// and to copy the behavior of the C implementation's `dataset_resize`
    fn grow(&mut self) {
        let buffer = &mut self.storage;
        let nchan = self.nchan.max(1);
        let old_spc = buffer.nframes_capacity * buffer.spf;
        // set reasonable maximum
        let new_spc = ((buffer.nframes_capacity * 2).max(64)) * buffer.spf;
        let nsamp = buffer.nframes_written * buffer.spf;

        // allocate and copy
        let mut new_data = RegValues::zeroed(self.reg_type, new_spc * nchan);
        new_data.copy_channels_from(&buffer.data, nchan, old_spc, new_spc, nsamp, 0);

        buffer.data = new_data;
        buffer.nframes_capacity = new_spc / buffer.spf;
    }

    /// trim to actual size.
    pub(crate) fn finish(self) -> RegData<RegValues> {
        let buffer = self.storage;
        let nchan = self.nchan.max(1);
        let nsamp = self.nsamp;

        // If buffer is larger than the actual number of frames:
        // Allocate and copy to exact size
        // If buffer is already the right size, simply move over register values into register data
        let data = if buffer.nframes_written < buffer.nframes_capacity {
            let src_spc = buffer.nframes_capacity * buffer.spf;
            let mut trimmed = RegValues::zeroed(self.reg_type, nsamp * nchan);
            trimmed.copy_channels_from(&buffer.data, nchan, src_spc, nsamp, nsamp, 0);
            trimmed
        } else {
            buffer.data
        };

        RegData {
            nchan: self.nchan,
            nsamp,
            reg_type: self.reg_type,
            storage: data,
        }
    }
}

// public-facing interface
// contains register values in column-major format
impl RegData<RegValues> {
    /// Consume and return the typed data.
    /// Bool registers are converted from U8 here for use with the binding libraries
    pub fn into_values(self) -> RegValues {
        if self.reg_type == RegType::Bool {
            if let RegValues::U8(bytes) = self.storage {
                return RegValues::Bool(bytes.into_iter().map(|b| b != 0).collect());
            }
        }
        self.storage
    }

    /// Concatenate data from multiple files into a single RegData.
    /// One allocation for the total size, then copies each file's
    /// per-channel data into the correct position. Like C's approach
    /// of pre-allocating for the total frame count.
    pub(crate) fn concatenate(parts: Vec<Self>) -> Self {
        assert!(!parts.is_empty());
        let nchan = parts[0].nchan;
        let nchan_max = nchan.max(1);
        let reg_type = parts[0].reg_type;
        let total_spc: usize = parts.iter().map(|p| p.nsamp).sum();

        let rt = parts[0].storage.reg_type();
        let mut out = RegValues::zeroed(rt, total_spc * nchan_max);
        let mut cursor: usize = 0;

        for part in &parts {
            let part_spc = part.nsamp;
            out.copy_channels_from(
                &part.storage,
                nchan_max,
                part_spc,
                total_spc,
                part_spc,
                cursor,
            );
            cursor += part_spc;
        }

        Self {
            nchan,
            nsamp: total_spc,
            reg_type,
            storage: out,
        }
    }
}

// typed data buffer
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
    /// Allocate a zeroed buffer. Bool maps to U8.
    fn zeroed(rt: RegType, n: usize) -> Self {
        match rt {
            RegType::UChar | RegType::Bool => Self::U8(vec![0; n]),
            RegType::Char => Self::I8(vec![0; n]),
            RegType::UShort => Self::U16(vec![0; n]),
            RegType::Short => Self::I16(vec![0; n]),
            RegType::UInt => Self::U32(vec![0; n]),
            RegType::Int => Self::I32(vec![0; n]),
            RegType::Float => Self::F32(vec![0.0; n]),
            RegType::Double => Self::F64(vec![0.0; n]),
            RegType::Utc => Self::Utc(vec![[0u32; 2]; n]),
        }
    }

    /// Copy `nsamp` samples per channel from `src` into `self`,
    /// where source and destination may have different total
    /// samples-per-channel.
    /// `dst_offset` is the sample offset within each destination channel.
    fn copy_channels_from(
        &mut self,
        src: &RegValues,
        nchan: usize,
        src_spc: usize,
        dst_spc: usize,
        nsamp: usize,
        dst_offset: usize,
    ) {
        let elem = src.element_size();
        let src_bytes = src.as_bytes();
        let dst_bytes = self.as_bytes_mut();
        let src_chan_bytes = src_spc * elem;
        let dst_chan_bytes = dst_spc * elem;
        let copy_bytes = nsamp * elem;
        let offset_bytes = dst_offset * elem;

        for ch in 0..nchan {
            dst_bytes[ch * dst_chan_bytes + offset_bytes
                ..ch * dst_chan_bytes + offset_bytes + copy_bytes]
                .copy_from_slice(&src_bytes[ch * src_chan_bytes..ch * src_chan_bytes + copy_bytes]);
        }
    }

    fn element_size(&self) -> usize {
        match self {
            Self::U8(_) | Self::I8(_) => 1,
            Self::U16(_) | Self::I16(_) => 2,
            Self::U32(_) | Self::I32(_) | Self::F32(_) => 4,
            Self::F64(_) | Self::Utc(_) => 8,
            Self::Bool(_) => unreachable!("Bool stored as U8 internally"),
        }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        use bytemuck::cast_slice_mut;
        match self {
            Self::U8(v) => v.as_mut_slice(),
            Self::I8(v) => cast_slice_mut(v.as_mut_slice()),
            Self::U16(v) => cast_slice_mut(v.as_mut_slice()),
            Self::I16(v) => cast_slice_mut(v.as_mut_slice()),
            Self::U32(v) => cast_slice_mut(v.as_mut_slice()),
            Self::I32(v) => cast_slice_mut(v.as_mut_slice()),
            Self::F32(v) => cast_slice_mut(v.as_mut_slice()),
            Self::F64(v) => cast_slice_mut(v.as_mut_slice()),
            Self::Utc(v) => cast_slice_mut(v.as_mut_slice()),
            Self::Bool(_) => unreachable!("Bool stored as U8 internally"),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        use bytemuck::cast_slice;
        match self {
            Self::U8(v) => v.as_slice(),
            Self::I8(v) => cast_slice(v.as_slice()),
            Self::U16(v) => cast_slice(v.as_slice()),
            Self::I16(v) => cast_slice(v.as_slice()),
            Self::U32(v) => cast_slice(v.as_slice()),
            Self::I32(v) => cast_slice(v.as_slice()),
            Self::F32(v) => cast_slice(v.as_slice()),
            Self::F64(v) => cast_slice(v.as_slice()),
            Self::Utc(v) => cast_slice(v.as_slice()),
            Self::Bool(_) => unreachable!("Bool stored as U8 internally"),
        }
    }

    fn reg_type(&self) -> RegType {
        match self {
            Self::U8(_) => RegType::UChar,
            Self::I8(_) => RegType::Char,
            Self::U16(_) => RegType::UShort,
            Self::I16(_) => RegType::Short,
            Self::U32(_) => RegType::UInt,
            Self::I32(_) => RegType::Int,
            Self::F32(_) => RegType::Float,
            Self::F64(_) => RegType::Double,
            Self::Bool(_) => RegType::Bool,
            Self::Utc(_) => RegType::Utc,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::regmap::RegBlockSpec;

    fn make_spec(reg_type_bits: u32, nchan: usize, spf: usize, ofs: usize) -> RegBlockSpec {
        let flags = 0x20000C; // FAST | RW
        let spec = [reg_type_bits | flags, 0x0F, 0, 0, nchan as u32, spf as u32];
        RegBlockSpec::new("test".into(), "board".into(), "block".into(), spec, ofs).unwrap()
    }

    #[test]
    fn scatter_frame_appends_multiple_frames_per_channel() {
        let spec = make_spec(0x2000, 2, 1, 0); // UChar, nchan=2, spf=1, ofs=0
        let mut data = RegData::<Buffer>::new(&spec, None, 2);

        data.scatter_frame(&[1, 10]);
        data.scatter_frame(&[2, 20]);

        let out = data.finish().into_values();

        match out {
            RegValues::U8(v) => {
                assert_eq!(
                    v,
                    vec![
                        1, 2, // ch0 across both frames
                        10, 20, // ch1 across both frames
                    ]
                );
            }
            _ => panic!("expected U8"),
        }
    }

    #[test]
    fn scatter_frame_respects_sparse_channel_selection() {
        let spec = make_spec(0x2000, 4, 1, 0); // UChar, nchan=4, ofs=0
        let mut data = RegData::<Buffer>::new(&spec, Some(vec![0, 2]), 1);

        data.scatter_frame(&[11, 22, 33, 44]);

        let out = data.finish().into_values();

        match out {
            RegValues::U8(v) => {
                assert_eq!(
                    v,
                    vec![
                        11, // selected source ch0
                        33, // selected source ch2
                    ]
                );
            }
            _ => panic!("expected U8"),
        }
    }

    #[test]
    fn scatter_frame_grows_buffer_and_preserves_existing_data() {
        let spec = make_spec(0x2000, 2, 1, 0); // UChar, nchan=2, ofs=0
        let mut data = RegData::<Buffer>::new(&spec, None, 1); // capacity=1, forces grow

        data.scatter_frame(&[1, 10]);
        data.scatter_frame(&[2, 20]);
        data.scatter_frame(&[3, 30]);

        let out = data.finish().into_values();

        match out {
            RegValues::U8(v) => {
                assert_eq!(v, vec![1, 2, 3, 10, 20, 30]);
            }
            _ => panic!("expected U8"),
        }
    }

    #[test]
    fn into_values_converts_bool_from_u8() {
        let data = RegData {
            nchan: 1,
            nsamp: 4,
            reg_type: RegType::Bool,
            storage: RegValues::U8(vec![0, 1, 2, 0]),
        };

        match data.into_values() {
            RegValues::Bool(v) => assert_eq!(v, vec![false, true, true, false]),
            _ => panic!("expected Bool"),
        }
    }

    #[test]
    fn concatenate_appends_channel_data_across_parts() {
        let p1 = RegData {
            nchan: 2,
            nsamp: 2,
            reg_type: RegType::UChar,
            storage: RegValues::U8(vec![1, 2, 10, 20]),
        };

        let p2 = RegData {
            nchan: 2,
            nsamp: 2,
            reg_type: RegType::UChar,
            storage: RegValues::U8(vec![3, 4, 30, 40]),
        };

        let out = RegData::concatenate(vec![p1, p2]).into_values();

        match out {
            RegValues::U8(v) => {
                assert_eq!(v, vec![1, 2, 3, 4, 10, 20, 30, 40]);
            }
            _ => panic!("expected U8"),
        }
    }
}
