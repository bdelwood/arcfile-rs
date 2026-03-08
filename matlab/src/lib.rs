mod error;
mod logging;

use rustmex::numeric::Numeric;
use rustmex::prelude::*;
use std::path::Path;

use arcfile_rs::arcfile::ArcFileLoader;
use arcfile_rs::register::{RegData, RegValues};
use rustmex::MatlabClass;
use rustmex::cell::CellArray;
use rustmex::char::CharArray;
use rustmex::structs::{ScalarStruct, Struct};
use std::ffi::CStr;
use std::ffi::CString;

use std::time::Instant;

use error::MexResult;
use jiff::{Timestamp, civil::DateTime, tz::TimeZone};
use log::{debug, info};
use logging::init_logger;

#[rustmex::entrypoint(catch_panic)]
fn readarc_rs(lhs: Lhs, rhs: Rhs) -> rustmex::Result<()> {
    init_logger();
    // Get filename as char array.
    // We'll convert it to a String
    let filename_mx = rhs
        .get(0)
        .error_if_missing("readarc:no_file", "Missing filename")?;

    let filename_char = CharArray::from_mx_array(filename_mx)
        .mex_err("readarc:bad_type", "Filename must be a char array")?;

    // Convert char to cstring to String
    let filename: String = filename_char.get_cstring().to_string_lossy().into_owned();

    // Get requested time range
    // C impl expects %Y-%b-%d:%T
    let t1: Timestamp = if let Some(&ts_mex) = rhs.get(1) {
        parse_timestamp(ts_mex)?
    } else {
        Timestamp::MIN
    };

    let t2: Timestamp = if let Some(&ts_mex) = rhs.get(2) {
        parse_timestamp(ts_mex)?
    } else {
        Timestamp::MAX
    };

    debug!("t1: {:?}", t1);
    debug!("t2: {:?}", t2);

    // way more complicated than it should be
    // but to match C impl we have to accept single char arrays and cells of char arrays
    let filters: Vec<String> = match rhs.get(3) {
        // nothing passed, just get empty vec
        None => vec![],
        // get mx object if user passed 3rd arg
        Some(mx) => {
            let mx = *mx;
            // check if it's a cell
            if let Ok(cell) = CellArray::from_mx_array(mx) {
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
    // borrow all the Strings
    let filters_ref: Vec<&str> = filters.iter().map(String::as_str).collect();

    // Open and parse
    let t_open = Instant::now();

    let loader = ArcFileLoader::new(t1..=t2, &filters_ref)
        .mex_err("readarc:loader", "Failed to make loader")?;
    let mut af = loader
        .open(&[Path::new(&filename).to_path_buf()])
        .mex_err("readarc:open", "Failed to open")?;

    debug!("Open: {:?}", t_open.elapsed());

    let t_convert = Instant::now();

    // Like Python bindings:
    // avoid copying, prefer to have mex take ownership
    let regtree = af.into_tree();

    // make top level stuct
    let mut map_struct = make_scalar_struct(regtree.keys())?;
    // loop over maps
    for (map_name, boards) in regtree {
        let mut board_struct = make_scalar_struct(boards.keys())?;
        // loop over boards
        for (board_name, blocks) in boards {
            let mut block_struct = make_scalar_struct(blocks.keys())?;
            // loop over blocks
            for (block_name, block) in blocks {
                // actually extract and convert data type
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
        // fill in map strict
        map_struct.set(to_cstring(&map_name)?.as_c_str(), board_struct.into_inner())?;
    }

    // Write to first output slot
    // if let Some because user may not have asked for first slot in assignment
    if let Some(ret) = lhs.get_mut(0) {
        ret.replace(map_struct.into_inner());
    }

    debug!("Convert: {:?}", t_convert.elapsed());

    Ok(())
}

// Conversion from Rust representation to Mex.
trait ToMex {
    fn into_mex(self) -> rustmex::Result<MxArray>;
}

impl ToMex for RegData {
    // TODO: Transpose to column-major; values are mixed up because we unpack RegValues as row major
    fn into_mex(mut self) -> rustmex::Result<MxArray> {
        // basically make_array from Python bindings
        // with some extra care to handle errors, since we have
        // to handle these more explicitly with rustmex than with PyO3
        let nchan = self.nchan();
        let nsamp = self.nsamp();
        let data = self.data();

        macro_rules! make_numeric {
            ($v:expr) => {{
                let mx = Numeric::new($v.into_boxed_slice(), &[nsamp, nchan])
                    .mex_err("readarc:alloc", "Failed to create array")?;

                Ok(mx.into_inner())
            }};
        }

        // row-major...
        match data {
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
            // However, Matlab expects (MJD, time of day) packed into a single uint64
            // Matlab does something like
            // tmp(1:2:end)  % odd indices = low word = MJD
            // tmp(2:2:end)  % even indices = high word = time_of_day_ms
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
        .mex_err("readarc:bad_time", "Unable to datetime string")?;
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
        .map(|k| to_cstring(&k))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let refs: Vec<&CStr> = cstring.iter().map(|s| s.as_c_str()).collect();
    Ok(Struct::new(&[1, 1], &refs).into_scalar().unwrap())
}
