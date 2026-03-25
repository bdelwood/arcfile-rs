# arcfile-rs

[![CI status][ci-img]][ci-url]
[![Documentation][doc-img]][doc-url]

[ci-img]: https://img.shields.io/github/actions/workflow/status/bdelwood/arcfile-rs/ci.yaml?branch=master&style=for-the-badge&label=CI
[ci-url]: https://github.com/bdelwood/arcfile-rs/actions/workflows/ci.yaml
[doc-img]: https://img.shields.io/badge/docs-arcfile--rs-4d76ae?style=for-the-badge
[doc-url]: https://bdelwood.github.io/arcfile-rs/arcfile_rs/index.html

A Rust implementation of the BICEP/Keck GCP arcfile binary data format reader, with bindings for MATLAB (via rustmex) and Python (via PyO3). 

## Building

### Prerequisites

You'll need the Rust toolchain for the core library, plus the following for the bindings:

- **MATLAB**: a MATLAB installation with `mex` (only tested on R2021a and R2024b).
- **Python**: Python 3.10+ for the abi3-compatible wheels, or 3.14t for free-threaded builds. Install [uv](https://docs.astral.sh/uv/). If you want to build alternative wheels or build profiles, install [maturin](https://www.maturin.rs/).

### MEX bindings

Build the MEX binary:
```bash
cargo build --release -p arcfile-mex
```

Make it available to your MATLAB instance, e.g. by symlinking into a directory on your MATLAB path:
```bash
ln -sf $(realpath target/release/libarcfile_mex.so) $MATLABPATH/readarc_rs.mexa64
```

### Python bindings

Build and install with `uv`:

```bash
uv build --wheel py/
uv pip install target/wheels/arcfile_rs-*.whl
```
The default release profile will build wheels compatible with Python `>= 3.10`. 

For free-threaded wheels, targeting Python 3.14t:

```bash
maturin build --release -m py/Cargo.toml -i python3.14t
uv pip install target/wheels/arcfile_rs-*cp314t*.whl
```

### Compatibility builds

If you need to compile this project for compatibility with older versions of glibc, use the build image, which is based on PyPA's `manylinux2014`.


Build the MEX binary:
```bash
podman run --rm \
    -v "$PWD:/src" \
    -v $MATLABROOT:/opt/matlab:ro \
    -w /src \
    ghcr.io/bdelwood/arcfile-rs-build:latest \
    cargo build --release -p arcfile-mex
```
where `$MATLABROOT` is the output of running `matlabroot` from MATLAB.

If using singularity:

```bash
singularity run \
    --no-home \
    --writable-tmpfs \
    --bind "$PWD:/src" \
    --bind $MATLABROOT:/opt/matlab:ro \
    docker://ghcr.io/bdelwood/arcfile-rs-build:latest \
    bash -lc 'cd /src && cargo build --release -p arcfile-mex'
```

For the Python wheels, change the container command to `bash -lc 'cd /src && uv build --wheel py/'`.
```bash
podman run --rm \
    -v "$PWD:/src" \
    -w /src \
    ghcr.io/bdelwood/arcfile-rs-build:latest \
    bash -lc 'cd /src && uv build --wheel py/'
```


If you want to build the image locally:
```bash
podman build -t arcfile-rs-build .
```