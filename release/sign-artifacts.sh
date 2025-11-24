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

# Usage: sign-artifacts.sh <output-dir> [<gpg-passphrase>]

out_dir="${1:?output dir required}"
gpg_passphrase="${2:-}"

shopt -s nullglob
files=("${out_dir}"/*.tar.gz "${out_dir}"/*.zip)
shopt -u nullglob

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No release artifacts found in ${out_dir}"
  exit 1
fi

for f in "${files[@]}"; do
  if [[ -n "${gpg_passphrase}" ]]; then
    gpg --batch --yes --pinentry-mode loopback --passphrase "${gpg_passphrase}" --armor --detach-sign "$f"
  else
    gpg --batch --yes --pinentry-mode loopback --armor --detach-sign "$f"
  fi
  shasum -a 512 "$f" > "${f}.sha512"
done
