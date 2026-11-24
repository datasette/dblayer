#!/bin/bash
# The whole run, end to end, with teardown guaranteed on exit or Ctrl-C:
#
#   fly/bench.sh [REF] [quick|full] [tests]
#
# create app and volume -> build and push the image -> start the machine ->
# wait, fetch results into benchmarks/results/ -> destroy everything.
set -uo pipefail
cd "$(dirname "$0")"
trap './teardown.sh' EXIT
./create.sh || exit 1
./build-image.sh || exit 1
./launch.sh "$@" || exit 1
./fetch.sh
