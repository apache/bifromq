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
Usage: $(basename "$0") <release-branch> [--upload] [-rc <number>] [<svn-username> <svn-password>]
  <release-branch>   Release branch in format release-v<major>.<minor>.x (e.g. release-v4.0.x)
  --upload           (Optional) Upload artifacts to Apache dist/dev SVN
  -rc <number>       (Optional) Append -RC<number> suffix to tag when provided
  <svn-username>      (Optional) SVN username for committing to Apache Dev repo
  <svn-password>      (Optional) SVN password for committing to Apache Dev repo

Example:
  $(basename "$0") release-v4.0.x --upload -rc 1 my_user my_password
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
RC_NUMBER=""
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

verify_gpg_access() {
  local sign_cmd=(gpg --armor --sign)
  if ! echo test | "${sign_cmd[@]}" >/dev/null 2>&1; then
    echo "ERROR: GPG signing failed. Ensure gpg-agent is running and passphrase is cached."
    exit 1
  fi
}

shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --upload)
      UPLOAD=true
      shift
      ;;
    -rc)
      if [[ -z "$2" ]]; then
        echo "ERROR: -rc requires a number argument"
        usage
        exit 1
      fi
      if [[ ! "$2" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: -rc number must be a positive integer"
        usage
        exit 1
      fi
      RC_NUMBER="$2"
      shift 2
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
verify_gpg_access

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
if [[ -n "$RC_NUMBER" ]]; then
  TAG="${TAG}-RC${RC_NUMBER}"
fi
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
MVN_ARGS=(-Pbuild-release -DskipTests -Dgpg.useagent=true)
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
bash "${SCRIPT_DIR}/sign-artifacts.sh" "$WORKDIR"

  if [ "$UPLOAD" = true ]; then
    UPLOAD_DIR="${REVISION}"
    if [[ -n "$RC_NUMBER" ]]; then
      UPLOAD_DIR="${REVISION}-RC${RC_NUMBER}"
    fi
    SVN_TMP=$(mktemp -d)
    cd "$SVN_TMP"
    svn checkout --depth empty "$ASF_SVN_DEV_URL" "$SVN_TMP"
    if svn ls "${ASF_SVN_DEV_URL}/${UPLOAD_DIR}" >/dev/null 2>&1; then
      svn update --set-depth infinity "${UPLOAD_DIR}"
      svn rm --force "${UPLOAD_DIR}"
    else
      svn mkdir "${UPLOAD_DIR}"
    fi
    mkdir -p "${UPLOAD_DIR}"
    for f in "$WORKDIR"/*; do
      if [[ -f "$f" ]]; then
        cp "$f" "${UPLOAD_DIR}/"
      fi
    done
    svn add --force "${UPLOAD_DIR}"
    svn status
    if [ "$USERNAME" = "" ]; then
      svn commit -m "Add release ${UPLOAD_DIR}" || exit
    else
    svn commit -m "Add release ${UPLOAD_DIR}" --username "${USERNAME}" --password "${PASSWORD}" || exit
  fi
  echo "Artifacts uploaded to SVN dev: ${UPLOAD_DIR}"
else
  echo "Artifacts generated under target/output and copied to $WORKDIR. Use --upload to push to SVN."
fi

echo "========================================================="
echo "Apache BifroMQ release ${REVISION} build completed."
echo "========================================================="
