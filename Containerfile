FROM quay.io/pypa/manylinux2014_x86_64

ENV PATH="/opt/rh/devtoolset-10/root/usr/bin:${PATH}"

# install rust toolchain 
RUN export CARGO_HOME=/opt/cargo RUSTUP_HOME=/opt/rustup && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    chmod -R a+rx /opt/cargo /opt/rustup

ENV RUSTUP_HOME="/opt/rustup"
ENV CARGO_HOME="/tmp/cargo"
ENV PATH="/opt/cargo/bin:/opt/matlab/bin:${PATH}"

# For Python bindings, manylinux ships with uv.

WORKDIR /src
