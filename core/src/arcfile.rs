use crate::error::{ArcError, ArcResult};
use crate::register::TypedRegData;
use crate::regmap::RegBlockSpec;
use crate::regmap::{Endianness, parse_regmap};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use log::{debug, info};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub use crate::regmap::RegType;

pub struct ArcHeader {
    pub frame_len: usize,
    pub frame0_ofs: usize,
    arrmap_rev: u32,
    endianness: Endianness,
    raw: [u32; 6],
}

pub struct ArcFile {
    pub header: ArcHeader,
    pub num_frames: usize,
    pub registers: HashMap<String, Register>,
}

pub struct Register {
    pub spec: RegBlockSpec,
    pub data: Option<TypedRegData>,
}

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveRecordType {
    SizeRecord = 0,
    ArrayMapRecord = 1,
    FrameRecord = 2,
}

impl TryFrom<u32> for ArchiveRecordType {
    type Error = ArcError;

    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(ArchiveRecordType::SizeRecord),
            1 => Ok(ArchiveRecordType::ArrayMapRecord),
            2 => Ok(ArchiveRecordType::FrameRecord),
            _ => Err(ArcError::Format(format!("Unknown record type: {v}"))),
        }
    }
}

#[derive(Debug, Clone)]
enum FileType {
    Plain,
    Gzip,
    Bzip2,
}

impl TryFrom<&Path> for FileType {
    type Error = ArcError;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .ok_or(ArcError::Format(format!("Missing file extension.")))?;
        match ext {
            "gz" => Ok(Self::Gzip),
            "bz2" => Ok(Self::Bzip2),
            "dat" => Ok(Self::Plain),
            _ => Err(ArcError::Format(format!("Unknown file format: {ext}"))),
        }
    }
}

fn open_reader(path: &Path) -> ArcResult<Box<dyn Read>> {
    let fi_type = FileType::try_from(path)?;

    let fi = File::open(path)?;

    match fi_type {
        FileType::Gzip => Ok(Box::new(GzDecoder::new(BufReader::new(fi)))),
        FileType::Bzip2 => Ok(Box::new(BzDecoder::new(BufReader::new(fi)))),
        FileType::Plain => Ok(Box::new(BufReader::new(fi))),
    }
}

fn read_header(reader: &mut dyn Read) -> ArcResult<ArcHeader> {
    let mut buf = [0u8; 24];
    reader.read_exact(&mut buf)?;

    let mut header: Vec<u32> = buf
        .chunks_exact(4)
        .map(|u| u32::from_le_bytes(u.try_into().unwrap()))
        .collect();

    // Like in RWO's implementation,
    // if we're le, this will be small.
    let header_is_native = header[0] <= 0x00010000;

    let header_endian = if header_is_native {
        Endianness::native()
    } else {
        header.iter_mut().for_each(|h| *h = h.swap_bytes());
        Endianness::native().swap()
    };

    let ArchiveRecordType::SizeRecord = ArchiveRecordType::try_from(header[1])? else {
        return Err(ArcError::Corrupted(
            "Malformed arcfile: size should be first record.".to_string(),
        ));
    };

    let ArchiveRecordType::ArrayMapRecord = ArchiveRecordType::try_from(header[4])? else {
        return Err(ArcError::Corrupted(
            "Malformed arcfile: array map should be second record.".to_string(),
        ));
    };

    // offset for frame0_ofs
    let record_size = header[0];
    let arrmap_rev = header[1];

    Ok(ArcHeader {
        frame_len: header[2].wrapping_sub(8) as usize,
        frame0_ofs: header[3].wrapping_add(record_size) as usize,
        endianness: header_endian,
        arrmap_rev,
        raw: *header.as_array().unwrap(),
    })
}

impl ArcFile {
    pub fn open(path: &Path) -> ArcResult<Self> {
        info!("Opening arcfile.");
        // open reader
        let mut reader = open_reader(path)?;

        // pass buffer to read header
        let header = read_header(reader.as_mut())?;

        // read register map
        // get register list
        let mut regmap_buf = vec![0u8; header.frame0_ofs - 24];
        reader.read_exact(&mut regmap_buf)?;

        let regs = parse_regmap(&regmap_buf, header.endianness)?;

        // Determine which registers to load
        // TODO: add filtering here
        let archived: Vec<&RegBlockSpec> = regs.iter().filter(|r| r.do_arc()).collect();

        // Create typed builders for each register
        let mut builders: Vec<TypedRegData> = archived
            .iter()
            .map(|r| TypedRegData::empty(r.typeword.reg_type))
            .collect();

        // read frames
        // make buffer
        let mut frame_buf = vec![0u8; header.frame_len];
        let mut num_frames = 0;

        // loop reader until EOF
        while Self::read_frame(reader.as_mut(), &mut frame_buf)? {
            // for each frame read loop over register and parse into concrete types
            for (builder, reg) in builders.iter_mut().zip(archived.iter()) {
                builder.push_frame(&frame_buf[reg.ofs..reg.ofs + reg.frame_size()]);
            }
            num_frames += 1;
        }

        // throw everything into a hashmap
        // keys=register full name, values=typed register data
        let mut data_map: HashMap<String, TypedRegData> = archived
            .iter()
            .zip(builders)
            .map(|(reg, builder)| (reg.full_name(), builder))
            .collect();

        // copy C behavior: add empty registers to hashmap
        let mut registers = HashMap::with_capacity(regs.len());

        for spec in regs {
            let name = spec.full_name();
            let data = data_map.remove(&name);
            registers.insert(name, Register { spec, data });
        }

        // return ArcFile struct
        Ok(ArcFile {
            header,
            num_frames,
            registers,
        })
    }

    fn read_frame(reader: &mut dyn Read, buf: &mut [u8]) -> ArcResult<bool> {
        match reader.read_exact(buf) {
            Ok(()) => Ok(true),
            // gracefully handle where decoder reaches EOF
            // return false indicating we've reached the end of the file
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            // pass along ant other errors
            Err(e) => Err(e.into()),
        }
    }

    pub fn get(&self, name: &str) -> Option<&Register> {
        self.registers.get(name)
    }
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Register> {
        self.registers.get_mut(name)
    }

    /// List register names.
    pub fn register_names(&self) -> Vec<&String> {
        self.registers.keys().collect()
    }
}
