use crate::error::{ArcError, ArcResult};
use crate::register::{RegData, RegValues};
use crate::regmap::RegBlockSpec;
use crate::regmap::{Endianness, parse_regmap};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap};
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
    pub data: Option<RegData>,
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

type RegisterTree = BTreeMap<String, BTreeMap<String, BTreeMap<String, RegData>>>;

impl ArcFile {
    // TODO: add logs with time to open?
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
        let mut archived: Vec<(RegBlockSpec, RegValues)> = regs
            .into_iter()
            .filter(|r| r.do_arc())
            .map(|r| {
                let builder = RegValues::empty(r.typeword.reg_type);
                (r, builder)
            })
            .collect();

        // read frames
        // make buffer
        let mut frame_buf = vec![0u8; header.frame_len];
        let mut num_frames = 0;

        // loop reader until EOF
        while Self::read_frame(reader.as_mut(), &mut frame_buf)? {
            // for each frame read loop over register and parse into concrete types
            for (spec, builder) in archived.iter_mut() {
                builder.push_frame(&frame_buf[spec.ofs..spec.ofs + spec.frame_size()]);
            }
            num_frames += 1;
        }

        // throw everything into a hashmap
        // keys=register full name, values=typed register data
        let registers: HashMap<String, Register> = archived
            .into_iter()
            .map(|(spec, data)| {
                let nsamp = num_frames * spec.spf.max(1);
                let nchan = spec.nchan;
                (
                    spec.full_name(),
                    // TODO: both Register and RegData should probably have constructors
                    Register {
                        spec,
                        data: Some(RegData { data, nsamp, nchan }),
                    },
                )
            })
            .collect();

        // return ArcFile struct
        Ok(ArcFile {
            header,
            num_frames,
            registers,
        })
    }

    pub fn into_tree(&mut self) -> RegisterTree {
        let mut root = RegisterTree::new();

        for (name, reg) in self.registers.iter_mut() {
            let parts: Vec<&str> = name.split('.').collect();
            // TODO: ignore registers which don't have map.board.block, should raise error
            if parts.len() < 3 {
                continue;
            }

            if let Some(data) = reg.data.take() {
                root.entry(parts[0].to_string())
                    .or_default()
                    .entry(parts[1].to_string())
                    .or_default()
                    .insert(parts[2].to_string(), data);
            }
        }
        root
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

    // Methods for interfacing with flat register map
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
