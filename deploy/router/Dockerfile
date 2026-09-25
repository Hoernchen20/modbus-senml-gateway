# Builds the complete Alpine rootfs for the router's container runtime
# (armv7): the gateway cross-compiled as a static musl binary, installed
# into the official Alpine minirootfs together with a start script.
# Site config is not built in; the router mounts it at runtime.
#
# Follows the router manufacturer's example scripts, but everything runs
# on the x86_64 build host: Rust cross-compiles (zig provides the armv7 C
# compiler and linker for ring), and packages are installed into the
# armv7 rootfs by the host's apk, so no qemu is needed.
#
# Build context is the repository root. Use deploy/router/build.sh, which
# also copies the resulting alpine-rootfs.tgz out of the image.

ARG ALPINE_VERSION=3.22.6

# ---- 1. Cross-compile the gateway ----
FROM rust:1-alpine3.22 AS build
ARG RUST_TARGET=armv7-unknown-linux-musleabihf
RUN apk add --no-cache musl-dev zig \
 && rustup target add "$RUST_TARGET" \
 && cargo install --locked cargo-zigbuild
WORKDIR /src
# Fetch dependencies in their own layer so code changes don't re-download.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
 && cargo fetch --locked --target "$RUST_TARGET" \
 && rm -r src
COPY src src
RUN cargo zigbuild --release --locked --target "$RUST_TARGET" \
 && cp "target/$RUST_TARGET/release/modbus-senml-gateway" /modbus-senml-gateway

# ---- 2. Assemble the rootfs ----
FROM alpine:3.22 AS rootfs
ARG ALPINE_VERSION
ARG ALPINE_ARCH=armv7
# Extra Alpine packages for the router container (space separated). The
# gateway binary itself needs none. Installed with --no-scripts because
# armv7 install scripts can't run on the build host; packages that depend
# on an install script (e.g. to create a user) need extra steps here.
ARG PACKAGES="tzdata"

RUN apk add --no-cache curl tar

WORKDIR /work
RUN set -e; \
    branch="v${ALPINE_VERSION%.*}"; \
    file="alpine-minirootfs-${ALPINE_VERSION}-${ALPINE_ARCH}.tar.gz"; \
    url="https://dl-cdn.alpinelinux.org/alpine/${branch}/releases/${ALPINE_ARCH}"; \
    curl -fsSLO "$url/$file" -O "$url/$file.sha256"; \
    sha256sum -c "$file.sha256"; \
    mkdir /rootfs; \
    tar xzf "$file" -C /rootfs

# apk uses the rootfs's own repositories, keys and arch (armv7). Upgrade
# first to pick up security fixes released after the minirootfs.
RUN apk --root /rootfs --no-cache --no-scripts upgrade \
 && if [ -n "$PACKAGES" ]; then apk --root /rootfs --no-cache --no-scripts add $PACKAGES; fi

COPY --from=build /modbus-senml-gateway /rootfs/usr/local/bin/modbus-senml-gateway
COPY deploy/router/start.sh /rootfs/etc/start.sh
COPY deploy/router/run-gateway.sh /rootfs/usr/local/sbin/run-gateway
# Reference only: the site config is not part of the image. The router
# mounts it at runtime under /dev/container_config/files (see README.md).
COPY config.example.toml /rootfs/usr/local/share/modbus-gateway/config.example.toml

RUN set -e; \
    chmod 0755 /rootfs/etc/start.sh /rootfs/usr/local/sbin/run-gateway \
               /rootfs/usr/local/bin/modbus-senml-gateway; \
    mkdir -p /rootfs/etc/init.d /out; \
    tar czf /out/alpine-rootfs.tgz --numeric-owner -C /rootfs .
