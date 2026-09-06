# docsql multi-arch build.
# docker.io is unreachable in this environment, so the build runs on the
# Azure Linux base from mcr.microsoft.com (already available locally) with
# the Rust toolchain installed from rustup inside the image.
#
# Build stage: full test suite runs here — a failing test fails the build.
FROM mcr.microsoft.com/azurelinux/base/core:3.0 AS builder
RUN tdnf install -y gcc binutils glibc-devel libstdc++-devel ca-certificates curl tar gzip which && tdnf clean all
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.1 --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Test gate: any failing test fails the build (exit code propagates).
RUN cargo test --workspace --release
RUN cargo build --release -p docsql-server -p docsql-cli

# Runtime stage: server + cli only.
FROM mcr.microsoft.com/azurelinux/base/core:3.0
RUN tdnf install -y ca-certificates libstdc++ && tdnf clean all
COPY --from=builder /build/target/release/docsql-server /usr/local/bin/docsql-server
COPY --from=builder /build/target/release/docsql-cli /usr/local/bin/docsql-cli
EXPOSE 7600
ENTRYPOINT ["docsql-server"]
