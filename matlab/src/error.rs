type MexErr = rustmex::message::AdHoc<&'static str, String>;

pub(crate) trait MexResult<T> {
    fn mex_err(self, id: &'static str, msg: &str) -> Result<T, MexErr>;
}

impl<T, E: std::fmt::Display> MexResult<T> for Result<T, E> {
    fn mex_err(self, id: &'static str, msg: &str) -> Result<T, MexErr> {
        self.map_err(|e| rustmex::message::AdHoc(id, format!("{e}\n{}", msg)))
    }
}
