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

usage() {
  echo "Usage: $(basename "$0") [-t tag] <path-to-apache-bifromq-VERSION.tar.gz>" >&2
  exit 1
}

tag_override=""
target_arch="${TARGETARCH:-}"

while getopts ":t:a:h" opt; do
  case "$opt" in
    t) tag_override="$OPTARG" ;;
    a) target_arch="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))

[[ $# -eq 1 ]] || usage

artifact_path="$1"

if [[ ! -f "$artifact_path" ]]; then
  echo "Error: artifact not found: $artifact_path" >&2
  exit 1
fi

artifact_dir="$(cd "$(dirname "$artifact_path")" && pwd)"
artifact_file="$(basename "$artifact_path")"

if [[ "$artifact_file" =~ ^apache-bifromq-(.+)\.tar\.gz$ ]]; then
  version="${BASH_REMATCH[1]}"
else
  echo "Error: artifact name must match apache-bifromq-<VERSION>.tar.gz" >&2
  exit 1
fi

sha_file="${artifact_dir}/${artifact_file}.sha512"
if [[ ! -f "$sha_file" ]]; then
  echo "Error: checksum file missing: ${sha_file}" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
dockerfile="${repo_root}/Dockerfile"

if [[ ! -f "$dockerfile" ]]; then
  echo "Error: Dockerfile not found at ${dockerfile}" >&2
  exit 1
fi

context_dir="$artifact_dir"
tag="${tag_override:-apache-bifromq:${version}}"

if [[ -z "$target_arch" ]]; then
  machine=$(uname -m)
  case "$machine" in
    x86_64|amd64) target_arch="amd64" ;;
    aarch64|arm64|arm64e) target_arch="arm64" ;;
    *) echo "Error: unsupported machine architecture: ${machine}. Set TARGETARCH explicitly." >&2; exit 1 ;;
  esac
fi

docker build -f "$dockerfile" \
  --build-arg BIFROMQ_VERSION="$version" \
  --build-arg TARGETARCH="$target_arch" \
  -t "$tag" \
  "$context_dir"
