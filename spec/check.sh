#!/usr/bin/env bash
# Model-check the spec with TLC in a container (no local Java needed).
# usage: spec/check.sh Safety.cfg [extra TLC args]
set -euo pipefail
cd "$(dirname "$0")"
TLA_VERSION=${TLA_VERSION:-v1.7.4}
mkdir -p .tools
[[ -f .tools/tla2tools.jar ]] || curl -fsSL --retry 3 -o .tools/tla2tools.jar \
  "https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"
cfg=${1:-Safety.cfg}; shift || true
engine=${CONTAINER_ENGINE:-$(command -v podman || command -v docker)}
exec "$engine" run --rm -v "$PWD":/spec:Z -w /spec docker.io/library/eclipse-temurin:21-jre \
  java -XX:+UseParallelGC -Xmx${TLC_HEAP:-8g} -cp .tools/tla2tools.jar tlc2.TLC \
  -workers ${TLC_WORKERS:-8} -deadlock -cleanup -metadir /tmp/tlc -config "$cfg" "$@" MC.tla
