# docsql multi-arch build.
# docker.io is unreachable in this environment, so the build runs on the
# Azure Linux base from mcr.microsoft.com (already available locally) with
# the Rust toolchain installed from rustup inside the image.
#
# Build stage: the full test suite runs here by default — a failing test
# fails the build. CI passes RUN_TESTS=false because it already ran cargo
# test natively before building the image.
ARG RUN_TESTS=true
FROM mcr.microsoft.com/azurelinux/base/core:3.0 AS builder
ARG RUN_TESTS
RUN tdnf install -y gcc binutils glibc-devel libstdc++-devel ca-certificates curl tar gzip which && tdnf clean all
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.1 --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Test gate: any failing test fails the build (exit code propagates).
RUN if [ "$RUN_TESTS" = "true" ]; then cargo test --workspace --release; fi
RUN cargo build --release -p docsql-server -p docsql-cli -p docsql-web

# Runtime stage: server + cli only, running as an unprivileged user.
# /data is pre-owned so fresh named volumes inherit writable ownership.
FROM mcr.microsoft.com/azurelinux/base/core:3.0
RUN tdnf install -y ca-certificates libstdc++ && tdnf clean all
COPY --from=builder /build/target/release/docsql-server /usr/local/bin/docsql-server
COPY --from=builder /build/target/release/docsql-cli /usr/local/bin/docsql-cli
COPY --from=builder /build/target/release/docsql-web /usr/local/bin/docsql-web
RUN mkdir /data && chown 1000:1000 /data
USER 1000:1000
EXPOSE 7600 7700
ENTRYPOINT ["docsql-server"]
