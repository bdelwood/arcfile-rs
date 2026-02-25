use arcfile_rs::register::TypedRegData;
use log::{debug, info};
use numpy::PyArray1;
use pyo3::prelude::*;
use pyo3_log;

// Take approach that bindings just implement the trait they need
// to pass off the data from Rust.
trait ToNumpy {
    fn into_numpy<'py>(
        self,
        py: Python<'py>,
        nrow: usize,
        ncol: usize,
    ) -> PyResult<Bound<'py, PyAny>>;
}

impl ToNumpy for TypedRegData {
    fn into_numpy<'py>(
        self,
        py: Python<'py>,
        nrow: usize,
        ncol: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        macro_rules! make_array {
            ($v:expr) => {{
                let arr = PyArray1::from_vec(py, $v).into_any();
                if ncol == 1 {
                    Ok(arr)
                } else {
                    arr.call_method1("reshape", ((nrow, ncol),))
                }
            }};
        }

        match self {
            Self::U8(v) => make_array!(v),
            Self::I8(v) => make_array!(v),
            Self::U16(v) => make_array!(v),
            Self::I16(v) => make_array!(v),
            Self::U32(v) => make_array!(v),
            Self::I32(v) => make_array!(v),
            Self::F32(v) => make_array!(v),
            Self::F64(v) => make_array!(v),
            // TODO: check bool is implemented correctly
            Self::Bool(v) => make_array!(v),
            Self::Utc(v) => {
                let flat: Vec<u32> = v.iter().flat_map(|p| *p).collect();
                let arr = PyArray1::from_vec(py, flat);
                arr.call_method1("reshape", ((nrow, 2 as usize),))
            }
        }
    }
}

#[pyo3::pymodule]
mod arcfile {
    use super::*;
    use pyo3::exceptions::{PyIOError, PyKeyError};

    use std::path::PathBuf;

    use arcfile_rs::arcfile::ArcFile;

    #[pymodule_init]
    fn init(_m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();
        Ok(())
    }

    #[pyclass(name = "ArcFile")]
    struct PyArcFile {
        path: PathBuf,
        inner: Option<ArcFile>,
    }

    #[pymethods]
    impl PyArcFile {
        #[new]
        fn new(path: PathBuf) -> Self {
            Self { path, inner: None }
        }

        fn open(&mut self) -> PyResult<()> {
            let af = ArcFile::open(&self.path).map_err(|e| PyIOError::new_err(e.to_string()))?;
            self.inner = Some(af);
            Ok(())
        }

        #[getter]
        fn num_frames(&self) -> PyResult<usize> {
            Ok(self.inner()?.num_frames)
        }

        fn __getitem__<'py>(&mut self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
            let af = self.inner_mut()?;

            // need to copy before mutable borrow of af
            let num_frames = af.num_frames;

            let reg = af
                .get_mut(name)
                .ok_or_else(|| PyKeyError::new_err(name.to_string()))?;

            let data = reg
                .data
                .take()
                .ok_or_else(|| PyKeyError::new_err(format!("{name} empty.")))?;

            // handle converting types and passing off to numpy
            data.into_numpy(py, num_frames * reg.spec.spf.max(1), reg.spec.nchan)
        }

        // TODO: probably some other helper methods and
        // dunders we want, eg __len__
        fn keys(&self) -> PyResult<Vec<&String>> {
            Ok(self.inner()?.register_names())
        }
    }

    // internal helpers impls for pulling actual arcfile struct
    impl PyArcFile {
        fn inner(&self) -> PyResult<&ArcFile> {
            self.inner
                .as_ref()
                .ok_or_else(|| PyIOError::new_err("File not opened. Call .open() first."))
        }

        fn inner_mut(&mut self) -> PyResult<&mut ArcFile> {
            self.inner
                .as_mut()
                .ok_or_else(|| PyIOError::new_err("File not opened. Call .open() first."))
        }
    }
}
