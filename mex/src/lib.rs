mod error;
mod logging;

use rustmex::numeric::Numeric;
use rustmex::prelude::*;
use std::path::PathBuf;

use arcfile_core::arcfile::ArcFileLoader;
use arcfile_core::register::{RegData, RegValues};
use rustmex::MatlabClass;
use rustmex::NewEmpty;
use rustmex::cell::CellArray;
use rustmex::char::CharArray;
use rustmex::structs::{ScalarStruct, Struct};
use std::ffi::CStr;
use std::ffi::CString;

use std::time::Instant;

use error::MexResult;
use jiff::{Timestamp, civil::DateTime, tz::TimeZone};
use log::{debug, info, trace};
use logging::init_logger;

#[rustmex::entrypoint(catch_panic)]
fn readarc_rs(lhs: Lhs, rhs: Rhs) -> rustmex::Result<()> {
    init_logger();
    debug!("readarc_rs called");

    let t_total = Instant::now();

    // Get filename. Accepts a single char array or a cell array of char arrays.
    // Single string can be a directory or file path.
    // Cell array allows passing multiple explicit file paths.
    #[allow(clippy::get_first)]
    let filename_mx = rhs
        .get(0)
        .error_if_missing("readarc:no_file", "Missing filename")?;

    let paths: Vec<PathBuf> = if let Ok(cell) = CellArray::from_mx_array(filename_mx) {
        debug!(
            "Parsing filenames from cell array with {} element(s)",
            cell.numel()
        );
        (0..cell.numel())
            .flat_map(|i| cell.get(i).ok().flatten())
            .map(|item| {
                let chars = CharArray::from_mx_array(item)
                    .mex_err("readarc:bad_type", "Cell elements must be strings.")?;
                Ok(PathBuf::from(
                    chars.get_cstring().to_string_lossy().into_owned(),
                ))
            })
            .collect::<std::result::Result<Vec<_>, rustmex::Error>>()?
    } else if let Ok(chars) = CharArray::from_mx_array(filename_mx) {
        vec![PathBuf::from(
            chars.get_cstring().to_string_lossy().into_owned(),
        )]
    } else {
        return Err(rustmex::message::AdHoc(
            "readarc:bad_type",
            "Filename must be a char array or cell array of char arrays.",
        )
        .into());
    };
    debug!("Parsed {} path(s)", paths.len());

    // Get requested time range
    // C impl expects %Y-%b-%d:%T
    // [], '', etc (empty array/char) carries same meaning as None
    let t1: Timestamp = match rhs.get(1) {
        Some(&ts_mex) if !ts_mex.is_empty() => parse_timestamp(ts_mex)?,
        _ => Timestamp::MIN,
    };

    let t2: Timestamp = match rhs.get(2) {
        Some(&ts_mex) if !ts_mex.is_empty() => parse_timestamp(ts_mex)?,
        _ => Timestamp::MAX,
    };

    debug!("Parsed time range: {:?}..={:?}", t1, t2);
    // way more complicated than it should be
    // but to match C impl we have to accept single char arrays and cells of char arrays
    let filters: Vec<String> = match rhs.get(3) {
        None => {
            vec![]
        }
        Some(mx) => {
            let mx = *mx;

            if let Ok(cell) = CellArray::from_mx_array(mx) {
                debug!(
                    "Parsing filters from cell array with {} element(s)",
                    cell.numel()
                );
                // it's a cell array
                // loop through elements
                (0..cell.numel())
                    // unwrap options, skips cell elements that are empty
                    .flat_map(|i| cell.get(i).ok().flatten())
                    // convert char arrays inside to strings
                    .map(|item| {
                        let chars = CharArray::from_mx_array(item)
                            .mex_err("readarc:bad_filter", "Cell elements must be strings.")?;
                        Ok(chars.get_cstring().to_string_lossy().into_owned())
                    })
                    // mush into a vec of strings
                    .collect::<std::result::Result<Vec<_>, rustmex::Error>>()?
            // try to convert to char array
            } else if let Ok(chars) = CharArray::from_mx_array(mx) {
                debug!("Parsing single string filter");
                // it's a char array
                // just directly return a Vec<String>
                vec![chars.get_cstring().to_string_lossy().into_owned()]
            // bad input
            } else {
                return Err(rustmex::message::AdHoc(
                    "readarc:bad_filter",
                    "Filter must be char or cell array.",
                )
                .into());
            }
        }
    };
    debug!("Parsed {} filter(s)", filters.len());
    // borrow all the Strings
    let filters_ref: Vec<&str> = filters.iter().map(String::as_str).collect();

    // Start timer for file open time
    let t_open = Instant::now();

    let loader = ArcFileLoader::new(t1..=t2, &filters_ref)
        .mex_err("readarc:loader", "Failed to make loader")?;
    let mut af = loader
        .load(&paths)
        .mex_err("readarc:load", "Failed to load")?;

    debug!(
        "File open completed in {:.2}s",
        t_open.elapsed().as_secs_f64()
    );

    // If the ArcFile is empty, we should return
    // follows behavior in `mex_readarc.c` line 47
    if af.registers.is_empty() {
        let empty = Struct::new_empty();

        if let Some(ret) = lhs.get_mut(0) {
            debug!("No registers. Returning empty struct array.");
            ret.replace(empty.into_inner());
        }

        return Ok(());
    }

    let t_convert = Instant::now();

    // Like Python bindings:
    // avoid copying, prefer to have mex take ownership.
    let regtree = af.into_tree();

    trace!("into_tree: {:.3}s", t_convert.elapsed().as_secs_f64());

    let t_mx = Instant::now();

    // make top level struct
    let mut map_struct = make_scalar_struct(regtree.keys())?;
    // loop over maps
    for (map_name, boards) in regtree {
        let mut board_struct = make_scalar_struct(boards.keys())?;
        // loop over boards
        for (board_name, blocks) in boards {
            let mut block_struct = make_scalar_struct(blocks.keys())?;
            // loop over blocks
            for (block_name, block) in blocks {
                // convert to mxArray
                let mx = block.into_mex()?;
                // fill in block struct
                block_struct.set(to_cstring(&block_name)?.as_c_str(), mx)?;
            }
            // fill in board struct
            board_struct.set(
                to_cstring(&board_name)?.as_c_str(),
                block_struct.into_inner(),
            )?;
        }
        // fill in map struct
        map_struct.set(to_cstring(&map_name)?.as_c_str(), board_struct.into_inner())?;
    }

    trace!(
        "MATLAB struct assembly: {:.3}s",
        t_mx.elapsed().as_secs_f64()
    );

    // Write to first output slot
    // if let Some because user may not have asked for first slot in assignment
    if let Some(ret) = lhs.get_mut(0) {
        trace!("Writing output to lhs[0]");
        ret.replace(map_struct.into_inner());
    }

    info!(
        "readarc_rs completed successfully in {:.2}s",
        t_total.elapsed().as_secs_f64()
    );

    Ok(())
}

// Conversion from Rust representation to Mex.
trait ToMex {
    fn into_mex(self) -> rustmex::Result<MxArray>;
}

impl ToMex for RegData<RegValues> {
    fn into_mex(self) -> rustmex::Result<MxArray> {
        let nsamp = self.nsamp;
        let nchan = if nsamp != 0 {
            self.nchan
        // mimics C behavior where empty registers return [0,0] sized arrays
        } else {
            0
        };

        macro_rules! make_numeric {
            ($v:expr) => {{
                let mx = Numeric::new($v.into_boxed_slice(), &[nsamp, nchan])
                    .mex_err("readarc:alloc", "Failed to create array")?;
                Ok(mx.into_inner())
            }};
        }

        match self.into_values() {
            RegValues::U8(v) => make_numeric!(v),
            RegValues::I8(v) => make_numeric!(v),
            RegValues::U16(v) => make_numeric!(v),
            RegValues::I16(v) => make_numeric!(v),
            RegValues::U32(v) => make_numeric!(v),
            RegValues::I32(v) => make_numeric!(v),
            RegValues::F32(v) => make_numeric!(v),
            RegValues::F64(v) => make_numeric!(v),
            RegValues::Bool(v) => make_numeric!(v),

            // Utc is Vec<[u32; 2]>,
            // Matlab expects (MJD, time of day) packed into a single uint64
            RegValues::Utc(v) => {
                let n = v.len();
                let packed: Vec<u64> = v
                    .iter()
                    .map(|p| (p[0] as u64) | ((p[1] as u64) << 32))
                    .collect();

                let mx = Numeric::new(packed.into_boxed_slice(), &[n, 1])
                    .mex_err("readarc:alloc", "Failed to create array")?;

                Ok(mx.into_inner())
            }
        }
    }
}

fn parse_timestamp(ts_mex: &mxArray) -> rustmex::Result<Timestamp> {
    let chars = CharArray::from_mx_array(ts_mex)
        .mex_err("readarc:bad_time", "Time must be a char array")?;
    let char_str = chars.get_cstring().to_string_lossy().into_owned();
    let time = DateTime::strptime("%Y-%b-%d:%T", char_str)
        .mex_err("readarc:bad_time", "Unable to parse datetime string")?;
    let ts = TimeZone::UTC
        .to_timestamp(time)
        .mex_err("readarc:bad_time", "Unable to convert")?;

    Ok(ts)
}

fn to_cstring(s: &str) -> rustmex::Result<CString> {
    Ok(CString::new(s).mex_err("readarc:cstring", "Name contains null byte")?)
}

fn make_scalar_struct<'a, K: Iterator<Item = &'a String>>(
    keys: K,
) -> rustmex::Result<ScalarStruct<MxArray>> {
    let cstring: Vec<CString> = keys
        .map(|k| to_cstring(k))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let refs: Vec<&CStr> = cstring.iter().map(|s| s.as_c_str()).collect();
    Ok(Struct::new(&[1, 1], &refs).into_scalar().unwrap())
}
