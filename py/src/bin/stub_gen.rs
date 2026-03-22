use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = arcfile::stub_info()?;
    stub.generate()?;
    Ok(())
}
