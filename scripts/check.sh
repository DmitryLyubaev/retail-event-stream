#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .local/m2
if [ "$#" -eq 0 ]; then set -- spotless:check clean verify; fi
exec docker run --rm --network host --memory 2g --cpus 2 \
  --user "$(id -u):$(id -g)" --group-add "$(stat -c %g /var/run/docker.sock)" \
  --label portfolio.project=p22 --label portfolio.role=build \
  -e TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1 -e MAVEN_OPTS="-Duser.home=/tmp -Xmx1024m" \
  -v "$PWD:/workspace" -v /var/run/docker.sock:/var/run/docker.sock -w /workspace \
  --entrypoint mvn \
  maven:3.10.0-eclipse-temurin-25-noble@sha256:98104f84952dfc8df0cbf5933d6d460f58cd299d7457ff5645c271853f3d8b6e \
  -B -ntp --settings /workspace/.mvn/settings.xml -Dmaven.repo.local=/workspace/.local/m2 "$@"
