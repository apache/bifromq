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
  echo "Usage: $(basename "$0") [-t tag] [-P platforms] [-p] [-o output] <path-to-apache-bifromq-VERSION.tar.gz>" >&2
  exit 1
}

tag_override=""
platforms="${PLATFORMS:-linux/amd64,linux/arm64}"
push_image="${PUSH_IMAGE:-}"
output_opt="${OUTPUT_OPT:-}"

while getopts ":t:P:o:ph" opt; do
  case "$opt" in
    t) tag_override="$OPTARG" ;;
    P) platforms="$OPTARG" ;;
    o) output_opt="$OPTARG" ;;
    p) push_image="true" ;;
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

if [[ -z "$push_image" ]]; then
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    push_image="true"
  else
    push_image="false"
  fi
fi

if [[ -n "$output_opt" && "$push_image" == "true" ]]; then
  echo "Error: use either --output (-o) or --push (-p), not both." >&2
  exit 1
fi

platform_count=1
if [[ "$platforms" == *","* ]]; then
  platform_count=2
fi

if [[ "$platform_count" -gt 1 && "$push_image" != "true" && -z "$output_opt" ]]; then
  echo "Error: multi-arch build requires --push (-p) or --output (-o)." >&2
  exit 1
fi

buildx_output_args=()
if [[ -n "$output_opt" ]]; then
  buildx_output_args+=(--output "$output_opt")
elif [[ "$push_image" == "true" ]]; then
  buildx_output_args+=(--push)
else
  buildx_output_args+=(--load)
fi

docker buildx build -f "$dockerfile" \
  --platform "$platforms" \
  --build-arg BIFROMQ_VERSION="$version" \
  -t "$tag" \
  "${buildx_output_args[@]}" \
  "$context_dir"
