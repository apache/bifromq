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
Usage: $(basename "$0") <release-branch> [<svn-username> <svn-password>]
  <release-branch>   Release branch in format release-v<major>.<minor>.x (e.g. release-v4.0.x)
  <svn-username>      (Optional) SVN username for committing to Apache Dev repo
  <svn-password>      (Optional) SVN password for committing to Apache Dev repo

Example:
  $(basename "$0") release-v4.0.x my_user my_password
EOF
}

# Show help if no arguments or help flag is given
if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

set -e

# =====================================================
# BifroMQ ASF Release Tool
# =====================================================

PROJECT_NAME="bifromq"
ASF_SVN_DEV_URL="https://dist.apache.org/repos/dist/dev/incubator/${PROJECT_NAME}"

BRANCH="$1"
USERNAME=$2
PASSWORD=$3

if [[ ! "$BRANCH" =~ ^release-v([0-9]+)\.([0-9]+)\.x$ ]]; then
  echo "ERROR: Branch name must follow release-v<major>.<minor>.x format (e.g. release-v4.0.x)"
  exit 1
fi

MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
MAJOR_MINOR="${MAJOR}.${MINOR}"

WORKDIR=$(pwd)
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

command -v gpg >/dev/null || { echo "GPG is required but not installed."; exit 1; }

echo "Cloning repository..."
cd "$TMPDIR"
git clone https://github.com/apache/${PROJECT_NAME}.git repo
cd repo
git checkout "$BRANCH"

echo "Locating latest tag for branch $BRANCH..."
LATEST_TAG=$(git tag --list "v${MAJOR_MINOR}.*" --sort=-v:refname | head -n 1)
if [ -z "$LATEST_TAG" ]; then
  echo "ERROR: No matching tag found."
  exit 1
fi
echo "Found tag: $LATEST_TAG"
git checkout "$LATEST_TAG"

# remove prefix 'v' from tag
VERSION="${LATEST_TAG#v}"

POM_VERSION=$(xmllint --xpath "string(//project/properties/revision)" pom.xml)
if [ "$POM_VERSION" != "$VERSION" ]; then
  echo "ERROR: POM revision ($POM_VERSION) doesn't match tag version ($VERSION)"
  exit 1
fi

echo "ASF required file check..."
for f in LICENSE NOTICE DISCLAIMER; do
  [ -f "$f" ] || { echo "Missing $f file."; exit 1; }
done

SRC_DIR="${PROJECT_NAME}-${VERSION}-src"
SRC_TARBALL="${SRC_DIR}.tar.gz"
git archive --format=tar.gz --prefix="${SRC_DIR}/" -o "$WORKDIR/$SRC_TARBALL" "$LATEST_TAG"

echo "Building binary via Maven..."
mvn clean package -DskipTests

cd "$WORKDIR"
cp "$TMPDIR/repo/target/output/bifromq-*.*" "${WORKDIR}"

cd "$WORKDIR"
find . -maxdepth 1 -type f ! -name '*.asc' ! -name '*.sha512' -print0 | while IFS= read -r -d '' ARTIFACT; do
  gpg --armor --output "${ARTIFACT}.asc" --detach-sign "$ARTIFACT"
  shasum -a 512 "$ARTIFACT" > "${ARTIFACT}.sha512"
done

SVN_TMP=$(mktemp -d)
svn checkout "$ASF_SVN_DEV_URL" "$SVN_TMP"
mkdir -p "$SVN_TMP/$VERSION"
cp "$WORKDIR"/* "$SVN_TMP/$VERSION/"
cd "$SVN_TMP"
svn add --force "$VERSION"
svn status

if [ "$USERNAME" = "" ]; then
  svn commit -m "Add release ${VERSION}" || exit
else
  svn commit -m "Add release ${VERSION}" --username "${USERNAME}" --password "${PASSWORD}" || exit
fi

echo "========================================================="
echo "BifroMQ release $VERSION has been successfully created."
echo "========================================================="