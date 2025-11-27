#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: plugin-release.sh [options]
Options:
  --repo <path>          Deploy to local file repository (default: <root>/target/local-repo)
  --staging              Deploy to apache staging (no local repo); incompatible with --repo
  -h, --help             Show this help
Examples:
  plugin-release.sh
  plugin-release.sh --repo /tmp/m2-repo
  plugin-release.sh --staging
EOF
}

ALT_DEPLOY_REPO="${ROOT_DIR}/target/local-repo"
USE_STAGING=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      shift
      [[ $# -gt 0 ]] || { echo "Missing value for --repo" >&2; usage; exit 1; }
      ALT_DEPLOY_REPO="$1"
      USE_STAGING=false
      ;;
    --staging)
      USE_STAGING=true
      ALT_DEPLOY_REPO=""
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if $USE_STAGING && [[ -n "${ALT_DEPLOY_REPO}" ]]; then
  echo "Error: --repo and --staging cannot be used together." >&2
  usage
  exit 1
fi

ALT_DEPLOY_OPT=()
SKIP_REMOTE_STAGING_OPT=()
if $USE_STAGING; then
  SKIP_REMOTE_STAGING_OPT+=("-DskipRemoteStaging=false")
else
  ALT_DEPLOY_OPT+=("-DaltDeploymentRepository=local::file:${ALT_DEPLOY_REPO}")
  SKIP_REMOTE_STAGING_OPT+=("-DskipRemoteStaging=true")
fi

export GPG_TTY=${GPG_TTY:-$(tty 2>/dev/null || true)}
GPG_OPT=("-Dgpg.useagent=true" "-Dgpg.executable=gpg")

mvn -N -f "${ROOT_DIR}/pom.xml" \
  -DskipTests \
  "${SKIP_REMOTE_STAGING_OPT[@]}" \
  "${ALT_DEPLOY_OPT[@]}" \
  "${GPG_OPT[@]}" \
  deploy

mvn -N -f "${ROOT_DIR}/bifromq-plugin/pom.xml" \
  -DskipTests \
  "${SKIP_REMOTE_STAGING_OPT[@]}" \
  "${ALT_DEPLOY_OPT[@]}" \
  "${GPG_OPT[@]}" \
  deploy

mvn -f "${ROOT_DIR}/bifromq-plugin/plugin-release/pom.xml" \
  -Papache-release \
  -DskipTests \
  "${SKIP_REMOTE_STAGING_OPT[@]}" \
  "${ALT_DEPLOY_OPT[@]}" \
  "${GPG_OPT[@]}" \
  deploy
