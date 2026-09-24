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
# glibc-static: the release binaries are linked crt-static so the runtime
# stage can be `scratch` (image = the three binaries, nothing else).
RUN tdnf install -y glibc-static && tdnf clean all
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.1 --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
# Fully static glibc. Azure Linux 3.0 ships glibc 2.38, where nss_files and
# nss_dns live inside libc itself, so the static binaries still resolve
# /etc/hosts and the docker embedded DNS (service names in DOCSQL_PEERS and
# the compose healthchecks) at runtime. Target-scoped RUSTFLAGS (not the
# global env) keep host artifacts — proc-macro dylibs — dynamically linked,
# which their crate type requires. Both CI platforms are covered. Local dev
# builds stay dynamic — these flags exist only inside the image build.
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+crt-static"
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+crt-static"
# This environment's network resets HTTP/2 streams mid-download (crates.io,
# rustup, github alike); cargo over HTTP/1.1 with extra retries survives it.
ENV CARGO_HTTP_MULTIPLEXING=false
ENV CARGO_NET_RETRY=10
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Test gate and build. The explicit --target is load-bearing even though it
# equals the build host: only then does cargo scope the CARGO_TARGET_*_RUSTFLAGS
# above to target units and keep proc-macro dylibs dynamic (their crate type
# cannot link crt-static).
RUN HOST=$(rustc -vV | sed -n 's/^host: //p') \
  && if [ "$RUN_TESTS" = "true" ]; then cargo test --workspace --release --target "$HOST"; fi \
  && cargo build --release --target "$HOST" -p docsql-server -p docsql-cli -p docsql-web \
  && mkdir -p /build/out \
  && cp target/*/release/docsql-server target/*/release/docsql-cli target/*/release/docsql-web /build/out/
# Compose mount points must pre-exist in the runtime image owned by uid
# 1000 (a fresh named volume inherits the ownership of the mount-point
# directory in the image), and a fixed /etc/nsswitch.conf keeps name
# resolution deterministic on scratch (glibc's built-in default differs
# across versions). The scratch runtime has no shell, so everything is
# staged here.
RUN mkdir -p /build/rootfs/data /build/rootfs/auth /build/rootfs/etc \
  && printf 'hosts: files dns\n' > /build/rootfs/etc/nsswitch.conf

# Runtime stage: scratch — the image IS the payload. No base OS, no shell,
# no package manager, no ls/cat/getent: inspect the container from outside
# via `docker exec <container> docsql-cli …` or `docker cp` (daemon-side tar
# stream, needs no in-container binaries), and keep compose healthchecks in
# exec-form CMD — CMD-SHELL would silently fail.
FROM scratch
# Static glibc malloc still honors the arena cap: join/repair snapshot
# replay churns hundreds of thousands of short-lived allocations across
# threads; with 32 arenas a 500k-statement bootstrap grew past 6 GB and the
# container was OOM-killed, while capping arenas keeps the same replay at
# ~300 MB.
ENV MALLOC_ARENA_MAX=2
COPY --from=builder /build/out/docsql-server /usr/local/bin/docsql-server
COPY --from=builder /build/out/docsql-cli /usr/local/bin/docsql-cli
COPY --from=builder /build/out/docsql-web /usr/local/bin/docsql-web
COPY --from=builder /build/rootfs/etc/nsswitch.conf /etc/nsswitch.conf
COPY --from=builder --chown=1000:1000 /build/rootfs/data /data
COPY --from=builder --chown=1000:1000 /build/rootfs/auth /auth
USER 1000:1000
EXPOSE 7600 7700
ENTRYPOINT ["docsql-server"]
