#!/usr/bin/env bash
# Start the development MQTT broker (Mosquitto 2, MQTT 5) on 127.0.0.1:1883 with podman or docker.
set -euo pipefail
cd "$(dirname "$0")"
engine=${CONTAINER_ENGINE:-$(command -v podman || command -v docker)}
"$engine" rm -f octopus-mosquitto >/dev/null 2>&1 || true
"$engine" run -d --name octopus-mosquitto --network host \
  -v "$PWD/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro,Z" docker.io/library/eclipse-mosquitto:2 >/dev/null
for _ in $(seq 1 30); do
  "$engine" logs octopus-mosquitto 2>&1 | grep -q "running" && { echo "mosquitto ready on 127.0.0.1:1883"; exit 0; }
  sleep 1
done
echo "mosquitto did not start" >&2; "$engine" logs octopus-mosquitto >&2; exit 1
