#!/usr/bin/env bash
# Export .env for Compose interpolation as well as Airflow's container env_file.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -s .env ]]; then
  python3 scripts/setup_env.py
fi
set -a
source .env
set +a
export ASTRO_TELEMETRY_DISABLED=1
if [[ "${1:-}" == "dev" && ( "${2:-}" == "start" || "${2:-}" == "restart" ) ]]; then
  # Astro builds the Airflow image; build the additional labelled images here.
  docker build -t task3-mongodb:1.0.0 docker/mongodb
  docker build -t task3-mongo-express:1.0.0 docker/mongo-express
fi
if command -v astro >/dev/null 2>&1; then
  exec astro "$@"
fi
exec .tools/astro "$@"
