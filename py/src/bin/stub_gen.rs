use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = arcfile_py::stub_info()?;
    stub.generate()?;
    Ok(())
}
