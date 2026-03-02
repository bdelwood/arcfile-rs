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

use jiff::{Timestamp, civil::DateTime, tz::TimeZone};

use log::{LevelFilter, Log, Metadata, Record, debug, info};

// It's annoying to use eprintln, println, etc throughout
// to get matlab
// rustmex prelude includes println!
// let's wire that up to log
// by implementing our own logger
struct MatLabLogger;

impl Log for MatLabLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("[{}] {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

// static cause set_logger requires it to live forever
static LOGGER: MatLabLogger = MatLabLogger;

// quick helper to enable logging
// TODO: make log level configurable
fn init_logger() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Debug))
        .ok();
}

trait ToMex {
    fn into_mex(self) -> rustmex::Result<MxArray>;
}

impl ToMex for RegData {
    fn into_mex(self) -> rustmex::Result<MxArray> {
        // basically make_array from Python bindings
        // with some extra care to handle errors, since we have
        // to handle these more explicitly with rustmex than with PyO3
        let nchan = self.nchan();
        let nsamp = self.nsamp();
        let data = self.data();

        macro_rules! make_numeric {
            ($v:expr) => {{
                let mx = Numeric::new($v.into_boxed_slice(), &[nsamp, nchan]).map_err(
                    |_| -> rustmex::Error {
                        rustmex::message::AdHoc("readarc:alloc", "Failed to create array").into()
                    },
                )?;

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
            // ie pair per row
            RegValues::Utc(v) => {
                let n = v.len();

                // matlab column-major:
                // first all of column 1, then all of column 2
                let mut flat = Vec::<u32>::with_capacity(n * 2);
                flat.extend(v.iter().map(|p| p[0]));
                flat.extend(v.iter().map(|p| p[1]));

                let mx = Numeric::new(flat.into_boxed_slice(), &[n, 2]).map_err(
                    |_| -> rustmex::Error {
                        rustmex::message::AdHoc("readarc:alloc", "Failed to create array").into()
                    },
                )?;

                Ok(mx.into_inner())
            }
        }
    }
}

fn to_cstring(s: &str) -> rustmex::Result<CString> {
    CString::new(s)
        .map_err(|_| rustmex::message::AdHoc("readarc:cstring", "Name contains null byte").into())
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

#[rustmex::entrypoint(catch_panic)]
fn readarc_rs(lhs: Lhs, rhs: Rhs) -> rustmex::Result<()> {
    init_logger();
    // Get filename as char array.
    // We'll convert it to a String
    let filename_mx = rhs
        .get(0)
        .error_if_missing("readarc:no_file", "Missing filename")?;

    let filename_char = CharArray::from_mx_array(filename_mx).map_err(|_| {
        rustmex::message::AdHoc("readarc:bad_type", "Filename must be a char array")
    })?;

    // Convert char to cstring to String
    let filename: String = filename_char.get_cstring().to_string_lossy().into_owned();

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
                        let chars = CharArray::from_mx_array(item).map_err(|_| {
                            rustmex::message::AdHoc(
                                "readarc:bad_filter",
                                "Cell elements must be strings.",
                            )
                        })?;
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

    // TODO: handle these as args, ie rhs.get(1), rhs.get(2)
    let t1 = Timestamp::MIN;
    let t2 = Timestamp::MAX;

    let loader = ArcFileLoader::new(t1..=t2, &filters_ref).map_err(|e| {
        rustmex::message::AdHoc("readarc:loader", format!("Failed to make loader: {e}"))
    })?;
    let mut af = loader
        .open(&[Path::new(&filename).to_path_buf()])
        .map_err(|e| rustmex::message::AdHoc("readarc:open", format!("Failed to open: {e}")))?;

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

    // Write to first output
    // if let Some because user may not have asked for first slot in assignment
    if let Some(ret) = lhs.get_mut(0) {
        ret.replace(map_struct.into_inner());
    }

    debug!("Convert: {:?}", t_convert.elapsed());

    Ok(())
}
