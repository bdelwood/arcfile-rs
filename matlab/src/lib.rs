use rustmex::numeric::Numeric;
use rustmex::prelude::*;
use std::path::Path;

use arcfile_rs::arcfile::ArcFile;
use arcfile_rs::register::TypedRegData;
use rustmex::MatlabClass;
use rustmex::char::CharArray;

use std::time::Instant;

// TODO: make ToMx trait for type conversion mirrored off Python interface to use in if let Some block

#[rustmex::entrypoint]
fn readarc_rs(lhs: Lhs, rhs: Rhs) -> rustmex::Result<()> {
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

    // Do same raw mx array > char > cstring > String for register name
    let regname_mx = rhs
        .get(1)
        .error_if_missing("readarc:no_reg", "Missing register name")?;
    let regname_char = CharArray::from_mx_array(regname_mx).map_err(|_| {
        rustmex::message::AdHoc("readarc:bad_type", "Register name must be a char array")
    })?;
    let regname: String = regname_char.get_cstring().to_string_lossy().into_owned();

    // Open and parse
    let t0 = Instant::now();

    let mut af = ArcFile::open(Path::new(&filename))
        .map_err(|e| rustmex::message::AdHoc("readarc:open", format!("Failed to open: {e}")))?;

    // TODO: replace with proper logging
    eprintln!("Open: {:?}", t0.elapsed());

    let t1 = Instant::now();

    // Like Python bindings:
    // avoid copying, prefer to have mex  take ownership
    let reg = af.get_mut(&regname).ok_or_else(|| {
        rustmex::message::AdHoc("readarc:noreg", format!("Register not found: {}", regname))
    })?;

    let data = reg.data.take().ok_or_else(|| {
        rustmex::message::AdHoc("readarc:excluded", format!("{} is not archived.", regname))
    })?;

    // Write to first output
    // if let Some because user may not have asked for first slot in assignment
    if let Some(ret) = lhs.get_mut(0) {
        // basically make_array from Python bindsings
        // with some extra care to handle errors, since we have
        // to handle these more explicitly with rustmex than with PyO3
        macro_rules! write_numeric {
            ($v:expr) => {{
                let n = $v.len();
                let mx = Numeric::new($v.into_boxed_slice(), &[n, 1]).map_err(
                    |_| -> rustmex::Error {
                        rustmex::message::AdHoc("readarc:alloc", "Failed to create array").into()
                    },
                )?;
                ret.replace(mx.into_inner());
            }};
        }

        // TODO: implement rest of types
        // row-major...
        match data {
            TypedRegData::U32(v) => write_numeric!(v),
            TypedRegData::I32(v) => write_numeric!(v),
            TypedRegData::F32(v) => write_numeric!(v),
            TypedRegData::F64(v) => write_numeric!(v),

            // Assuming Utc is something like Vec<[u32; 2]>,
            // ie pair per row
            TypedRegData::Utc(v) => {
                let n = v.len();

                // MATLAB column-major:
                // first all of column 1, then all of column 2
                let mut flat = Vec::<u32>::with_capacity(n * 2);
                flat.extend(v.iter().map(|p| p[0]));
                flat.extend(v.iter().map(|p| p[1]));

                let mx = Numeric::new(flat.into_boxed_slice(), &[n, 2]).map_err(
                    |_| -> rustmex::Error {
                        rustmex::message::AdHoc("readarc:alloc", "Failed to create array").into()
                    },
                )?;

                ret.replace(mx.into_inner());
            }

            _ => rustmex::error!("readarc:type", "Unsupported register type"),
        }
    }

    eprintln!("Convert: {:?}", t1.elapsed());

    Ok(())
}
