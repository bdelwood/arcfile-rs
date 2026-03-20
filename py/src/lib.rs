use arcfile_rs::arcfile::ArcFileLoader;
use arcfile_rs::register::{RegData, RegValues};
use jiff::Timestamp;
use log::{debug, info};
use numpy::PyArray1;
use pyo3::prelude::*;
use pyo3::types::PyDateTime;
use pyo3::types::PyDict;
use std::time::Instant;

#[pyo3::pymodule]
mod arcfile {
    use super::*;
    use pyo3::exceptions::{PyIOError, PyKeyError};
    use std::path::PathBuf;

    #[pymodule_init]
    fn init(_m: &Bound<'_, PyModule>) -> PyResult<()> {
        // default logger max log level is debug
        // need to set it here to trace
        pyo3_log::Logger::new(_m.py(), pyo3_log::Caching::LoggersAndLevels)?
            .filter(log::LevelFilter::Trace)
            .install()
            .ok();
        Ok(())
    }

    #[pyclass(name = "ArcFile")]
    struct PyArcFile {
        #[pyo3(get)]
        paths: Vec<PathBuf>,
        dict: Py<PyDict>,
    }

    #[pymethods]
    impl PyArcFile {
        #[staticmethod]
        #[pyo3(signature = (path, t1=None, t2=None, filters=None))]
        fn load<'py>(
            py: Python<'py>,
            path: Bound<'py, PyAny>,
            t1: Option<Bound<'py, PyDateTime>>,
            t2: Option<Bound<'py, PyDateTime>>,
            filters: Option<Vec<String>>,
        ) -> PyResult<Self> {
            let t_total = Instant::now();
            debug!("ArcFile.load called");

            // parse paths
            let paths: Vec<PathBuf> = if let Ok(s) = path.extract::<PathBuf>() {
                vec![s]
            } else if let Ok(v) = path.extract::<Vec<PathBuf>>() {
                v
            } else {
                return Err(PyIOError::new_err(
                    "path must be a string, a Path, a list of strings, or a list of Paths",
                ));
            };
            debug!("Parsed {} path(s)", paths.len());

            let filters = filters.unwrap_or_default();
            let filter_refs: Vec<&str> = filters.iter().map(|s| s.as_str()).collect();
            debug!("Parsed {} filter(s)", filters.len());

            // Parse time range
            // None loads everything
            let ts1 = match t1 {
                Some(s) => parse_timestamp(&s)?,
                None => Timestamp::MIN,
            };
            let ts2 = match t2 {
                Some(s) => parse_timestamp(&s)?,
                None => Timestamp::MAX,
            };
            debug!("Time range: {:?}..={:?}", ts1, ts2);

            let t_open = Instant::now();

            let loader = ArcFileLoader::new(ts1..=ts2, &filter_refs)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let mut af = py.detach(|| {
                loader
                    .load(&paths)
                    .map_err(|e| PyIOError::new_err(e.to_string()))
            })?;

            debug!(
                "File open completed in {:.2}s",
                t_open.elapsed().as_secs_f64()
            );

            let t_convert = Instant::now();

            // nested map
            let regtree = af.into_tree();

            debug!("into_tree: {:.3}s", t_convert.elapsed().as_secs_f64());

            let t_dict = Instant::now();

            // make top level dict
            let map_dict = PyDict::new(py);
            // loop over maps
            for (map_name, boards) in regtree {
                let board_dict = PyDict::new(py);
                // loop over boards
                for (board_name, blocks) in boards {
                    let block_dict = PyDict::new(py);
                    // loop over blocks
                    for (block_name, data) in blocks {
                        // extract and convert data to np array
                        let arr = data.into_numpy(py)?;
                        // fill in block dict item
                        block_dict.set_item(&block_name, arr)?;
                    }
                    // fill in board dict item
                    board_dict.set_item(&board_name, block_dict)?;
                }
                // fill in map dict item
                map_dict.set_item(&map_name, board_dict)?;
            }

            debug!("Dict assembly: {:.3}s", t_dict.elapsed().as_secs_f64());
            info!(
                "ArcFile.load completed in {:.2}s",
                t_total.elapsed().as_secs_f64()
            );

            Ok(Self {
                paths,
                dict: map_dict.unbind(),
            })
        }

        fn to_dict<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
            self.dict.bind(py).clone()
        }

        fn __getitem__<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
            self.dict
                .bind(py)
                .get_item(name)?
                .ok_or_else(|| PyErr::new::<PyKeyError, _>(name.to_string()))
        }

        fn __len__<'py>(&self, py: Python<'py>) -> PyResult<usize> {
            Ok(self.dict.bind(py).len())
        }

        fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            Ok(self.dict.bind(py).call_method0("keys")?)
        }
    }
}

// Take approach that bindings just implement the trait they need
// to pass off the data from Rust.
trait ToNumpy {
    fn into_numpy<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

impl ToNumpy for RegData<RegValues> {
    fn into_numpy<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let nchan = self.nchan;
        let nsamp = self.nsamp;

        macro_rules! make_array {
            ($v:expr) => {{
                let arr = PyArray1::from_vec(py, $v).into_any();
                if nchan == 1 {
                    Ok(arr)
                } else {
                    arr.call_method1("reshape", ((nsamp, nchan),))
                }
            }};
        }

        match self.into_values() {
            RegValues::U8(v) => make_array!(v),
            RegValues::I8(v) => make_array!(v),
            RegValues::U16(v) => make_array!(v),
            RegValues::I16(v) => make_array!(v),
            RegValues::U32(v) => make_array!(v),
            RegValues::I32(v) => make_array!(v),
            RegValues::F32(v) => make_array!(v),
            RegValues::F64(v) => make_array!(v),
            RegValues::Bool(v) => make_array!(v),
            RegValues::Utc(v) => {
                let flat: Vec<u32> = v.iter().flat_map(|p| *p).collect();
                let arr = PyArray1::from_vec(py, flat);
                arr.call_method1("reshape", ((nsamp, 2 as usize),))
            }
        }
    }
}

fn parse_timestamp(dt: &Bound<'_, PyDateTime>) -> PyResult<Timestamp> {
    let secs: f64 = dt.call_method0("timestamp")?.extract()?;
    Timestamp::from_millisecond((secs * 1e3) as i64).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Timestamp conversion: {}", e))
    })
}
