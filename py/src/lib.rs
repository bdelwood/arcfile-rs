use arcfile_rs::arcfile::ArcFileLoader;
use arcfile_rs::register::{RegData, RegValues};
use jiff::{Timestamp, civil::DateTime, tz::TimeZone};
use log::{debug, info};
use numpy::PyArray1;
use pyo3::prelude::*;
use pyo3::types::PyDict;

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
            // TODO: check bool is implemented correctly
            RegValues::Bool(v) => make_array!(v),
            RegValues::Utc(v) => {
                let flat: Vec<u32> = v.iter().flat_map(|p| *p).collect();
                let arr = PyArray1::from_vec(py, flat);
                arr.call_method1("reshape", ((nsamp, 2 as usize),))
            }
        }
    }
}

#[pyo3::pymodule]
mod arcfile {
    use super::*;
    use pyo3::exceptions::{PyIOError, PyKeyError};
    use std::path::PathBuf;

    #[pymodule_init]
    fn init(_m: &Bound<'_, PyModule>) -> PyResult<()> {
        // set up logging
        pyo3_log::init();
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
        #[pyo3(signature = (path, filters=None))]
        fn load<'py>(
            py: Python<'py>,
            path: Bound<'py, PyAny>,
            filters: Option<Vec<String>>,
        ) -> PyResult<Self> {
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
            let filters = filters.unwrap_or_default();
            let filter_refs: Vec<&str> = filters.iter().map(|s| s.as_str()).collect();

            // TODO: handle as args
            let t1 = Timestamp::MIN;
            let t2 = Timestamp::MAX;

            let loader = ArcFileLoader::new(t1..=t2, &filter_refs)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let mut af = py.detach(|| {
                loader
                    .load(&paths)
                    .map_err(|e| PyIOError::new_err(e.to_string()))
            })?;

            // nested map
            let regtree = af.into_tree();

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
