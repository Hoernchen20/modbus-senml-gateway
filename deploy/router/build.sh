#!/bin/sh
# Builds dist/alpine-rootfs.tgz for the router (see README.md).
# Extra arguments are passed to docker build, e.g.
#   deploy/router/build.sh --build-arg PACKAGES="tzdata tcpdump"
set -eu

cd "$(dirname "$0")/../.."

image=modbus-gateway-router-build
docker build -f deploy/router/Dockerfile -t "$image" "$@" .

mkdir -p dist
container=$(docker create "$image")
trap 'docker rm -f "$container" >/dev/null' EXIT
docker cp "$container:/out/alpine-rootfs.tgz" dist/alpine-rootfs.tgz
echo "Built dist/alpine-rootfs.tgz"
