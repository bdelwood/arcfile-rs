// TODO:
// refactoring and cleanup
// implement listarc

use crate::error::{ArcError, ArcResult};
use crate::register::{Buffer, RegData, RegValues};
use crate::regmap::{Endianness, RegBlockSpec, parse_regmap};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use jiff::{Timestamp, civil::DateTime, tz::TimeZone};
use log::{debug, info, trace};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use crate::regmap::RegType;

/// Number of frames to read at a time.
/// Matches the C implementation's NBUFFRAMES.
const CHUNK_FRAMES: usize = 64;

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
    data: Option<RegData<RegValues>>,
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
        // If split_once is not None, we have a range of channel
        // use parse to parse string into usize
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
        // otherwise, we have a single channel
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

/// Enable conversion from string to typed FilterSpec
impl FromStr for FilterSpec {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // split parts, map.board.block
        // where block has optional channel selection: `[:]`
        let mut parts = s.split('.');

        // step through map, board, block and convert to string
        let map = parts.next().filter(|p| !p.is_empty()).map(str::to_string);
        let board = parts.next().filter(|p| !p.is_empty()).map(str::to_string);
        let block_with_chsel = parts.next().filter(|p| !p.is_empty());

        // parse block and ch
        let (block, channels) = match block_with_chsel {
            // pattern match to Some block
            Some(b) => {
                if let Some((name, rest)) = b.split_once('[') {
                    let rest = rest.strip_suffix(']').ok_or_else(|| {
                        // NB: this is a divergence in behavior from the C implementation
                        // arguably it's a bug to accept chsel strings that are malformed
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing closing ']'")
                    })?;

                    // not sure this is needed, but check if empty
                    let name = if name.is_empty() {
                        None
                    } else {
                        Some(name.to_string())
                    };

                    // "rest" is everything outside of name
                    // parse using
                    (name, Some(parse_chsel(rest)?))
                } else {
                    (Some(b.to_string()), None)
                }
            }
            // If no block, no channel sel either
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

/// Open the correct reader for the file's compression format.
fn open_reader(path: &Path) -> ArcResult<Box<dyn Read>> {
    let fi_type = FileType::try_from(path)?;

    let fi = File::open(path)?;

    match fi_type {
        FileType::Gzip => Ok(Box::new(GzDecoder::new(BufReader::new(fi)))),
        FileType::Bzip2 => Ok(Box::new(BzDecoder::new(BufReader::new(fi)))),
        FileType::Plain => Ok(Box::new(BufReader::new(fi))),
    }
}

/// Parse arcfile by streaming from file reader
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

    // RWO's implementation handles headers with alternative endianness
    // I don't think this is useful anymore,
    // but we want to keep as close to the C behavior as possible
    let header_endian = if header_is_native {
        Endianness::native()
    } else {
        header.iter_mut().for_each(|h| *h = h.swap_bytes());
        Endianness::native().swap()
    };

    // check header sections containing size and array map records
    // are as they're supposed to be
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
        // need to offset by a byte
        frame_len: header[2].wrapping_sub(8) as usize,
        // likewise, offset by the reported record sizes
        frame0_ofs: header[3].wrapping_add(record_size) as usize,
        endianness: header_endian,
        // included in MAP's Python implementation, not used
        arrmap_rev,
        raw: *header.as_array().unwrap(),
    })
}

/// Read the uncompressed size from a gzip file's trailer.
/// The last 4 bytes of a .gz file store the original size as a u32.
/// Useless if files are >4GB uncompressed
fn gz_uncompressed_size(path: &Path) -> Option<usize> {
    let mut f = File::open(path).ok()?;
    f.seek(SeekFrom::End(-4)).ok()?;
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf).ok()?;
    Some(u32::from_le_bytes(buf) as usize)
}

/// Estimate nframes for pre-allocation.
/// - Plain files: exact from file size.
/// - Gzip: exact from the uncompressed size in the gzip trailer.
/// - Bzip2: use ad hoc decompression ratios, since bz2 has no size trailer.
fn estimate_nframes(path: &Path, header: &ArcHeader) -> usize {
    if header.frame_len == 0 {
        return CHUNK_FRAMES;
    }

    // gzip
    // extract from the uncompressed size in the trailer
    if path.extension().is_some_and(|e| e == "gz") {
        if let Some(size) = gz_uncompressed_size(path) {
            if size > header.frame0_ofs {
                return (size - header.frame0_ofs) / header.frame_len;
            }
        }
        return CHUNK_FRAMES;
    }

    // plain or bz2
    // estimate from file size
    let fsize = fs::metadata(path).map(|m| m.len() as usize).unwrap_or(0);
    if fsize <= header.frame0_ofs {
        return CHUNK_FRAMES;
    }
    let estimate = (fsize - header.frame0_ofs) / header.frame_len;

    // bz2 has no size trailer
    // scale by some ad hoc compression ratio
    if path.extension().is_some_and(|e| e == "bz2") {
        estimate * 6
    } else {
        estimate
    }
}

pub fn list_and_sort(path: &Path, range: &RangeInclusive<Timestamp>) -> ArcResult<Vec<PathBuf>> {
    let mut entries: Vec<_> = fs::read_dir(path)?
        .filter_map(|res| {
            // Handle any DirEntries that are Err
            let path = res.ok()?;
            let entry = &path.file_name();
            let name = entry.to_str()?;
            // Filter out any files that aren't arcfiles (w/o .dat)
            let time = if name.contains(".dat") {
                // Skip any files that fail the date parser
                parse_date_arcfile(name).or_else(|| {
                    trace!("Skipping {:?}", name);
                    None
                })
            } else {
                None
            }?;
            // Map to timestamp, pathbuf tuple
            Some((time, path.path()))
        })
        .collect();

    // sort by timestamp parsed from filename
    entries.sort_by_key(|(t, _)| *t);

    // Get the first file before the time range
    // find stops at first
    let pre: Option<PathBuf> = entries
        .iter()
        .rev()
        .find(|(t, _)| t < range.start())
        .map(|(_, p)| p.clone());

    // Exclude any files outside of requested date range
    let fis: Vec<_> = pre
        .into_iter()
        .chain(
            entries
                .into_iter()
                .filter(|(t, _)| range.contains(t))
                .map(|(_, p)| p),
        )
        .collect();

    Ok(fis)
}

// TODO: refactor to make this unnecessary--we should use OsStr's file_prefix
fn parse_date_arcfile(name: &str) -> Option<Timestamp> {
    // Get date as file prefix
    let date_str = name.split('.').next().unwrap();
    let time = DateTime::strptime("%Y%m%d_%H%M%S", date_str).ok()?;
    TimeZone::UTC.to_timestamp(time).ok()
}

// Nested map to represent arcfile hierarchy
type RegisterTree = BTreeMap<String, BTreeMap<String, BTreeMap<String, RegData<RegValues>>>>;

/// Loader for ArcFiles
/// Keeps track of requested filters and file search time range
pub struct ArcFileLoader {
    filters: Vec<FilterSpec>,
    timerange: RangeInclusive<Timestamp>,
}

impl ArcFileLoader {
    pub fn new(timerange: RangeInclusive<Timestamp>, filters: &[&str]) -> ArcResult<Self> {
        let filters: Vec<FilterSpec> = filters
            .iter()
            .map(|f| f.parse())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { filters, timerange })
    }

    pub fn load(&self, paths: &[PathBuf]) -> ArcResult<ArcFile> {
        let t0 = std::time::Instant::now();

        // flatten paths
        let paths: Vec<PathBuf> = paths
            .iter()
            .flat_map(|p| {
                // if directory, list and sort, trim to within timerange
                if p.is_dir() {
                    list_and_sort(p, &self.timerange).unwrap_or_default()

                // otherwise, for individual files, just stuff into Vec
                // TODO: should trim on timerange here too
                } else {
                    vec![p.clone()]
                }
            })
            .collect();

        debug!(
            "list_and_sort: {:.2}s, {} files",
            t0.elapsed().as_secs_f64(),
            paths.len()
        );
        let t1 = std::time::Instant::now();

        // parallel open
        let afs = paths
            .par_iter()
            .map(|p| ArcFile::open(&p, &self.filters))
            .collect::<ArcResult<Vec<_>>>()?;

        trace!("par_iter open: {:.2}s", t1.elapsed().as_secs_f64());
        let t2 = std::time::Instant::now();

        // merge individual arcfiles into one virtual arcfile
        let af = ArcFile::concatenate(afs)?;

        trace!("concatenate: {:.2}s", t2.elapsed().as_secs_f64());

        Ok(af)
    }
}

impl ArcFile {
    pub fn open(path: &Path, filters: &[FilterSpec]) -> ArcResult<Self> {
        let fname = path.file_name().and_then(|f| f.to_str()).unwrap_or("?");
        info!("Opening arcfile {:?}.", fname);

        let t0 = std::time::Instant::now();

        // open reader
        let mut reader = open_reader(path)?;

        // pass buffer to read header
        let header = read_header(reader.as_mut())?;

        // read register map
        // get register list
        let mut regmap_buf = vec![0u8; header.frame0_ofs - 24];
        reader.read_exact(&mut regmap_buf)?;
        let regs = parse_regmap(&regmap_buf, header.endianness)?;

        trace!(
            "[{}] header + regmap: {:.2}s",
            fname,
            t0.elapsed().as_secs_f64()
        );
        let t1 = std::time::Instant::now();

        // Estimate nframes from file size for pre-allocation.
        // trim at end if needed
        let nframes_est = estimate_nframes(path, &header);
        trace!("[{}] estimated {} frames", fname, nframes_est);

        // Determine which registers to load and pre-allocate output buffers.
        let mut archived: Vec<(RegBlockSpec, RegData<Buffer>)> = regs
            .into_iter()
            // skip any registers marked as not archived
            .filter(|spec| spec.do_arc())
            // skip registers not matched by user provided filter
            .filter(|spec| filters.is_empty() || filters.iter().any(|f| f.matches(&spec)))
            // pre-allocate output buffer for each register
            // map register to (register specification, register data) tuple
            .map(|spec| {
                let channels = filters
                    .iter()
                    .find(|f| f.matches(&spec))
                    .and_then(|f| f.channels.clone());

                let reg_data = RegData::new(&spec, channels, nframes_est);
                (spec, reg_data)
            })
            .collect();

        trace!(
            "[{}] filter + alloc {} registers: {:.2}s",
            fname,
            archived.len(),
            t1.elapsed().as_secs_f64()
        );
        let t2 = std::time::Instant::now();

        // Read frames in chunks and scatter directly into output buffers.
        // Like the C implementation's approach: read a chunk,
        // scatter each frame's register data, reuse the read buffer.
        let mut chunk_buf = vec![0u8; header.frame_len * CHUNK_FRAMES];
        let mut nframes: usize = 0;

        // loop reader until EOF
        while let Some(frames_read) =
            Self::read_frames(reader.as_mut(), &mut chunk_buf, header.frame_len)?
        {
            // for each frame read loop over register and parse into concrete types
            // convert from row-major on-disk format to col major
            for frame_idx in 0..frames_read {
                let frame =
                    &chunk_buf[frame_idx * header.frame_len..(frame_idx + 1) * header.frame_len];

                for (_, reg_data) in archived.iter_mut() {
                    reg_data.scatter_frame(frame);
                }
            }

            nframes += frames_read;
        }

        trace!(
            "[{}] read + scatter {} frames: {:.2}s",
            fname,
            nframes,
            t2.elapsed().as_secs_f64()
        );
        let t3 = std::time::Instant::now();
        // throw everything into a hashmap
        // keys=register full name, values=Register struct (spec+data)
        let registers: HashMap<String, Register> = archived
            .into_iter()
            .map(|(spec, buffer)| {
                (
                    spec.full_name(),
                    Register {
                        spec,
                        data: Some(buffer.finish()),
                    },
                )
            })
            .collect();

        trace!(
            "[{}] finish {} registers: {:.2}s",
            fname,
            registers.len(),
            t3.elapsed().as_secs_f64()
        );
        debug!("[{}] total open: {:.2}s", fname, t0.elapsed().as_secs_f64());

        // return ArcFile struct
        Ok(ArcFile { header, registers })
    }

    /// Organize registers into a map.board.block tree.
    /// Data is already unpacked
    /// This should just reorganize ownership.
    pub fn into_tree(&mut self) -> RegisterTree {
        // Build nested BTreeMap
        let mut root = RegisterTree::new();

        // Loop over register hashmap items
        for (name, reg) in self.registers.iter_mut() {
            let parts: Vec<&str> = name.split('.').collect();
            // ignore registers which don't have map.board.block
            // Should this raise an error?
            if parts.len() < 3 {
                continue;
            }

            // extract actual data and build nested BTreeMap
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

    /// Take individually loaded arcfiles
    /// and concatenate them into a single, contiguous "virtual" arcfile.
    pub fn concatenate(files: Vec<Self>) -> ArcResult<Self> {
        let mut all_files: Vec<Self> = files.into_iter().collect();
        if all_files.is_empty() {
            return Err(ArcError::Format("No files.".to_string()));
        }
        if all_files.len() == 1 {
            // unwrap should be fine here, as all_files should not be empty
            return Ok(all_files.pop().unwrap());
        }

        // build virtual arcfile header
        let header = ArcHeader {
            frame_len: all_files[0].header.frame_len,
            frame0_ofs: all_files[0].header.frame0_ofs,
            arrmap_rev: all_files[0].header.arrmap_rev,
            endianness: all_files[0].header.endianness,
            raw: all_files[0].header.raw,
        };

        // For each register, collect data from all files then concatenate
        // in one allocation. Like C's pre-allocate-for-total approach.
        // collect parts sequentially
        // just moves pointers by using take
        let reg_names: Vec<String> = all_files[0].registers.keys().cloned().collect();
        let parts_by_reg: Vec<(String, RegBlockSpec, Vec<RegData<RegValues>>)> = reg_names
            .into_iter()
            .filter_map(|name| {
                let parts: Vec<RegData<RegValues>> = all_files
                    .iter_mut()
                    .filter_map(|af| af.registers.get_mut(&name)?.data.take())
                    .collect();

                // return early if we've got nothing
                if parts.is_empty() {
                    return None;
                }

                // reconstruct spec
                let spec = all_files
                    .iter()
                    .find_map(|af| af.registers.get(&name).map(|r| r.spec.clone()))
                    .unwrap();

                Some((name, spec, parts))
            })
            .collect();

        // do actual concatenation
        // by stuffing virtual arcfile into Hashmap
        // we've got rayon in scope, might as well use it
        let registers: HashMap<String, Register> = parts_by_reg
            .into_par_iter()
            .map(|(name, spec, parts)| {
                let data = if parts.len() == 1 {
                    parts.into_iter().next().unwrap()
                } else {
                    RegData::concatenate(parts)
                };
                (
                    name,
                    Register {
                        spec,
                        data: Some(data),
                    },
                )
            })
            .collect();

        Ok(ArcFile { header, registers })
    }

    /// Read up to a buffer's worth of frames from the reader.
    /// Returns Some(frames_read) or None at EOF.
    fn read_frames(
        reader: &mut dyn Read,
        buf: &mut [u8],
        frame_len: usize,
    ) -> ArcResult<Option<usize>> {
        let max_frames = buf.len() / frame_len;
        for i in 0..max_frames {
            let frame = &mut buf[i * frame_len..(i + 1) * frame_len];
            // read until EOF
            match reader.read_exact(frame) {
                Ok(()) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return if i == 0 { Ok(None) } else { Ok(Some(i)) };
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(Some(max_frames))
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
