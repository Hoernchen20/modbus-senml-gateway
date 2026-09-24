# Builds the gateway as a static musl binary (same as the deployment build)
# and runs it on plain Alpine. Build context is the repository root.

FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
# Cache mounts keep the crate registry and build artifacts between image
# builds, so only changed code is recompiled. The binary is copied out in
# the same step because the target dir is not part of the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
 && cp target/release/modbus-senml-gateway /modbus-senml-gateway

FROM alpine:3
COPY --from=build /modbus-senml-gateway /usr/local/bin/modbus-senml-gateway
ENTRYPOINT ["/usr/local/bin/modbus-senml-gateway"]
CMD ["/etc/modbus-gateway/config.toml"]
