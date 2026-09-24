# DocSQL multi-arch build.
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
# Compose mount points must pre-exist in the runtime image owned by uid
# 1000 (a fresh named volume inherits the ownership of the mount-point
# directory in the image); the distroless runtime below has no shell, so
# they are staged here.
RUN mkdir -p /build/rootfs/data /build/rootfs/auth

# Runtime stage: azurelinux distroless — glibc runtime only. No shell, no
# package manager, no ls/cat/getent: inspect the container from outside via
# `docker exec <container> docsql-cli …` or `docker cp` (daemon-side tar
# stream, needs no in-container binaries), and keep compose healthchecks in
# exec-form CMD — CMD-SHELL would silently fail. The binaries link only
# libc/libm/libgcc_s (all part of the distroless glibc runtime); the old
# base's libstdc++/ca-certificates/bourne-tool layer was dead weight, and
# nothing in the stack opens outbound TLS.
FROM mcr.microsoft.com/azurelinux/distroless/base:3.0
# glibc keeps freed memory in per-thread arenas (default cap = 8 × cores). The
# join/repair snapshot replay churns hundreds of thousands of short-lived
# allocations across threads; with 32 arenas a 500k-statement bootstrap grew
# past 6 GB and the container was OOM-killed, while capping arenas keeps the
# same replay at ~300 MB.
ENV MALLOC_ARENA_MAX=2
COPY --from=builder /build/target/release/docsql-server /usr/local/bin/docsql-server
COPY --from=builder /build/target/release/docsql-cli /usr/local/bin/docsql-cli
COPY --from=builder /build/target/release/docsql-web /usr/local/bin/docsql-web
COPY --from=builder --chown=1000:1000 /build/rootfs/data /data
COPY --from=builder --chown=1000:1000 /build/rootfs/auth /auth
USER 1000:1000
EXPOSE 7600 7700
ENTRYPOINT ["docsql-server"]
