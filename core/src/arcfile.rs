// TODO:
// implement timerange clipping
// refactoring and cleanup

use crate::error::{ArcError, ArcResult};
use crate::register::RegData;
use crate::regmap::RegBlockSpec;
use crate::regmap::{Endianness, parse_regmap};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use jiff::{Timestamp, civil::DateTime, tz::TimeZone};
use log::{debug, info};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::FromStr;

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

#[derive(Debug, Clone)]
pub struct FilterSpec {
    map: Option<String>,
    board: Option<String>,
    block: Option<String>,
    channels: Option<Vec<usize>>,
}

impl FilterSpec {
    pub fn matches(&self, r: &RegBlockSpec) -> bool {
        // Match on all parts
        part_match(&r.map_name, self.map.as_deref())
            && part_match(&r.board_name, self.board.as_deref())
            && part_match(&r.block_name, self.block.as_deref())
    }
}

// copy C behavior wrt wildcards and missing values
fn part_match(value: &str, filt: Option<&str>) -> bool {
    // If this filter part is missing, match all
    let Some(f) = filt else { return true };
    // If it is an empty string, match all
    if f.is_empty() {
        return true;
    };

    // Look for requested filter before *
    if let Some(prefix) = f.strip_suffix("*") {
        // match on prefix
        return value.starts_with(prefix);
    }

    // exact match for what is left
    value == f
}

// TODO: clean up errors here
// Don't think they should use ArcError, but maybe a different one?
fn parse_chsel(chsel: &str) -> Result<Vec<usize>, std::io::Error> {
    let mut channels = Vec::new();

    for part in chsel.split(".") {
        let part = part.trim();
        if let Some((start, end)) = part.split_once(":") {
            let start: usize = start
                .trim()
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let end: usize = end
                .trim()
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            channels.extend(start..=end);
        } else {
            let idx: usize = part
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            channels.push(idx);
        };
    }
    channels.dedup();
    Ok(channels)
}

impl FromStr for FilterSpec {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('.');

        let map = parts.next().filter(|p| !p.is_empty()).map(str::to_string);
        let board = parts.next().filter(|p| !p.is_empty()).map(str::to_string);
        let block_with_chsel = parts.next().filter(|p| !p.is_empty());

        let (block, channels) = match block_with_chsel {
            Some(b) => {
                if let Some((name, rest)) = b.split_once('[') {
                    let rest = rest.strip_suffix(']').ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing closing ']'")
                    })?;
                    let name = if name.is_empty() {
                        None
                    } else {
                        Some(name.to_string())
                    };
                    (name, Some(parse_chsel(rest)?))
                } else {
                    (Some(b.to_string()), None)
                }
            }
            None => (None, None),
        };

        Ok(Self {
            map,
            board,
            block,
            channels,
        })
    }
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

pub fn list_and_sort(path: &Path, range: &RangeInclusive<Timestamp>) -> ArcResult<Vec<PathBuf>> {
    let mut fis: Vec<_> = fs::read_dir(path)?
        .filter_map(|res| {
            // Handle any DirEntries that are Err
            let path = res.ok()?;
            let entry = &path.file_name();
            let name = entry.to_str()?;
            // Filter out any files that aren't arcfiles (w/o .dat)
            let time = if name.contains(".dat") {
                // Skip any files that fail the date parser
                parse_date_arcfile(name).or_else(|| {
                    debug!("Skipping {:?}", name);
                    None
                })
            } else {
                None
            }?;
            // Exclude any files outside of requested date range
            // and finally, map to OsString
            range.contains(&time).then_some(path.path())
        })
        .collect();

    // sort by datetime parsed from filename
    fis.sort_by(|a, b| {
        let dta = parse_date_arcfile(a.file_name().unwrap().to_str().unwrap()).unwrap();
        let dtb = parse_date_arcfile(b.file_name().unwrap().to_str().unwrap()).unwrap();
        dta.cmp(&dtb)
    });

    Ok(fis)
}

// TODO: refactor to make this unnecessary--we should use OsStr's file_prefix
fn parse_date_arcfile(name: &str) -> Option<Timestamp> {
    // Get date as file prefix
    let date_str = name.split('.').next().unwrap();
    let time = DateTime::strptime("%Y%m%d_%H%M%S", date_str).ok()?;
    TimeZone::UTC.to_timestamp(time).ok()
}

type RegisterTree = BTreeMap<String, BTreeMap<String, BTreeMap<String, RegData>>>;

pub struct ArcFileLoader {
    filters: Vec<FilterSpec>,
    timerange: RangeInclusive<Timestamp>,
}

impl ArcFileLoader {
    pub fn new(timerange: RangeInclusive<Timestamp>, filters: Vec<&str>) -> ArcResult<Self> {
        let filters: Vec<FilterSpec> = filters
            .iter()
            .map(|f| f.parse())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { filters, timerange })
    }

    pub fn open_dir(&self, path: &Path) -> ArcResult<Vec<ArcFile>> {
        let paths = list_and_sort(path, &self.timerange)?;

        let afs = paths
            .par_iter()
            .map(|p| ArcFile::open(&p, &self.filters))
            .collect::<ArcResult<Vec<_>>>()?;

        Ok(afs)
    }
}

impl ArcFile {
    // TODO: add logs with time to open?
    pub fn open(path: &Path, filters: &[FilterSpec]) -> ArcResult<Self> {
        info!("Opening arcfile {:?}.", path.file_name());
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
        let mut archived: Vec<(RegBlockSpec, RegData)> = regs
            .into_iter()
            // skip any registers marked as not archived
            .filter(|spec| spec.do_arc())
            // skip registers not matched by user provided filter
            .filter(|spec| filters.is_empty() || filters.iter().any(|f| f.matches(&spec)))
            // map register to (register, value) tuple
            .map(|spec| {
                let channels = filters
                    .iter()
                    .find(|f| f.matches(&spec))
                    .and_then(|f| f.channels.clone());

                let reg_data = RegData::new(&spec, channels);

                (spec, reg_data)
            })
            .collect();

        // read frames
        // make buffer
        let mut frame_buf = vec![0u8; header.frame_len];

        // loop reader until EOF
        while Self::read_frame(reader.as_mut(), &mut frame_buf)? {
            // for each frame read loop over register and parse into concrete types
            for (spec, reg_data) in archived.iter_mut() {
                let frame_slice = &frame_buf[spec.ofs..spec.ofs + spec.frame_size()];
                reg_data.push_frame(frame_slice, spec);
            }
        }

        // throw everything into a hashmap
        // keys=register full name, values=typed register data
        let registers: HashMap<String, Register> = archived
            .into_iter()
            .map(|(spec, reg_data)| {
                (
                    spec.full_name(),
                    // TODO: Register and RegData should probably have constructors
                    Register {
                        spec,
                        data: Some(reg_data),
                    },
                )
            })
            .collect();

        // return ArcFile struct
        let af = ArcFile { header, registers };

        debug!("{:?}", af.register_names());

        for (name, reg) in &af.registers {
            if let Some(ref data) = reg.data {
                debug!("{}: nchan={}", name, data.nchan());
            }
        }

        Ok(af)
    }

    pub fn into_tree(&mut self) -> RegisterTree {
        let mut root = RegisterTree::new();

        for (name, reg) in self.registers.iter_mut() {
            let parts: Vec<&str> = name.split('.').collect();
            // ignore registers which don't have map.board.block
            // Should this raise an error?
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
            // pass along any other errors
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
