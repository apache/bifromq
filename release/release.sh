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


# Print usage information
usage() {
  cat <<EOF
Usage: $(basename "$0") <release-branch> [--upload] [--gpg-passphrase <pass>] [<svn-username> <svn-password>]
  <release-branch>   Release branch in format release-v<major>.<minor>.x (e.g. release-v4.0.x)
  --upload           (Optional) Upload artifacts to Apache dist/dev SVN
  --gpg-passphrase   (Optional) GPG passphrase used for signing
  <svn-username>      (Optional) SVN username for committing to Apache Dev repo
  <svn-password>      (Optional) SVN password for committing to Apache Dev repo

Example:
  $(basename "$0") release-v1.0.x-incubating --upload my_user my_password
EOF
}

# Show help if no arguments or help flag is given
if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

set -e

# =====================================================
# Apache BifroMQ Release Tool
# =====================================================

PROJECT_NAME="bifromq"
ASF_SVN_DEV_URL="https://dist.apache.org/repos/dist/dev/incubator/${PROJECT_NAME}"

BRANCH="$1"
UPLOAD=false
USERNAME=""
PASSWORD=""
GPG_PASSPHRASE=""
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

verify_gpg_access() {
  local pass="$1"
  local sign_cmd=(gpg --armor --batch --yes --pinentry-mode loopback --sign)
  if [[ -n "$pass" ]]; then
    if ! echo test | "${sign_cmd[@]}" --passphrase "$pass" >/dev/null 2>&1; then
      echo "ERROR: Provided GPG passphrase is invalid or key is not accessible."
      exit 1
    fi
  else
    if ! echo test | "${sign_cmd[@]}" >/dev/null 2>&1; then
      echo "ERROR: GPG passphrase required. Provide it via --gpg-passphrase or ensure gpg-agent has it cached."
      exit 1
    fi
  fi
}

shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --upload)
      UPLOAD=true
      shift
      ;;
    --gpg-passphrase)
      shift
      if [[ -z "${1:-}" ]]; then
        echo "ERROR: --gpg-passphrase requires a value"
        usage
        exit 1
      fi
      GPG_PASSPHRASE="$1"
      shift
      ;;
    *)
      if [[ -z "$USERNAME" ]]; then
        USERNAME="$1"
      elif [[ -z "$PASSWORD" ]]; then
        PASSWORD="$1"
      else
        echo "ERROR: Too many arguments"
        usage
        exit 1
      fi
      shift
      ;;
  esac
done

if [[ ! "$BRANCH" =~ ^release-v([0-9]+)\.([0-9]+)\.x.*$ ]]; then
  echo "ERROR: Branch name must follow release-v<major>.<minor>.x format (e.g. release-v4.0.x or release-v1.0.0-incubating)"
  exit 1
fi

WORKDIR="${SCRIPT_DIR}/output"
mkdir -p "$WORKDIR"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

command -v gpg >/dev/null || { echo "GPG is required but not installed."; exit 1; }
export GPG_TTY=${GPG_TTY:-$(tty)}
gpg --list-secret-keys --with-colons | grep -q '^sec' || { echo "ERROR: No GPG secret key found for signing."; exit 1; }
verify_gpg_access "$GPG_PASSPHRASE"

echo "Cloning repository to temp dir..."
cd "$TMPDIR"
git clone https://github.com/apache/${PROJECT_NAME}.git repo
cd repo
git checkout "$BRANCH"

echo "Reading revision from POM..."
REVISION=$(xmllint --xpath "string(//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision'])" pom.xml)
if [[ -z "$REVISION" ]]; then
  echo "ERROR: Cannot read revision from pom.xml"
  exit 1
fi

TAG="v${REVISION}"
if ! git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "ERROR: Tag ${TAG} not found"
  exit 1
fi

if ! git merge-base --is-ancestor "${TAG}" "${BRANCH}"; then
  echo "ERROR: Tag ${TAG} is not reachable from branch ${BRANCH}"
  exit 1
fi

echo "Checking out tag ${TAG}..."
git checkout "${TAG}"

echo "ASF required file check..."
for f in LICENSE NOTICE DISCLAIMER; do
  [ -f "$f" ] || { echo "Missing $f file."; exit 1; }
done

echo "Running Maven build (build-release profile)..."
echo "You may be prompted for GPG passphrase by gpg-agent/pinentry."
MVN_ARGS=(-Pbuild-release -DskipTests)
if [[ -n "$GPG_PASSPHRASE" ]]; then
  MVN_ARGS+=("-Dgpg.passphrase=${GPG_PASSPHRASE}")
fi
mvn "${MVN_ARGS[@]}" clean verify

cd "$TMPDIR/repo"
ARTIFACTS=(target/output/apache-bifromq-*.tar.gz target/output/apache-bifromq-*.zip)
ls "${ARTIFACTS[@]}" 2>/dev/null || { echo "No artifacts found under target/output"; exit 1; }
for f in target/output/*; do
  if [[ -f "$f" ]]; then
    cp "$f" "$WORKDIR"
  fi
done

echo "Signing artifacts..."
bash "${SCRIPT_DIR}/sign-artifacts.sh" "$WORKDIR" "$GPG_PASSPHRASE"

if [ "$UPLOAD" = true ]; then
  SVN_TMP=$(mktemp -d)
  svn checkout "$ASF_SVN_DEV_URL" "$SVN_TMP"
  mkdir -p "$SVN_TMP/${REVISION}"
  cp target/output/* "$SVN_TMP/${REVISION}/"
  cd "$SVN_TMP"
  svn add --force "${REVISION}"
  svn status
  if [ "$USERNAME" = "" ]; then
    svn commit -m "Add release ${REVISION}" || exit
  else
    svn commit -m "Add release ${REVISION}" --username "${USERNAME}" --password "${PASSWORD}" || exit
  fi
  echo "Artifacts uploaded to SVN dev: ${REVISION}"
else
  echo "Artifacts generated under target/output and copied to $WORKDIR. Use --upload to push to SVN."
fi

echo "========================================================="
echo "Apache BifroMQ release ${REVISION} build completed."
echo "========================================================="
