#!/bin/sh
# Copyright 2023 The Dots Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -eu

usage() {
    echo "Usage: $0 <first node index> <last node index> [-- [args...]]"
}

if [ $# -lt 2 ]; then
    usage
    exit 1
fi

set -x

first=$1
last=$2

trap "trap '' TERM && kill 0 2>/dev/null" INT QUIT TERM EXIT

while [ "$#" -ge 1 ] && [ "$1" != '--' ]; do
    shift
done
if [ "$#" -ge 1 ]; then
    shift
fi

make -j cmd/dotsserver/dotsserver

i=$first
while [ "$i" -le "$last" ]; do
    ./cmd/dotsserver/dotsserver -config server_conf.yml -node_id "node$i" -listen_offset "$i" "$@" 2>&1 | sed -E "s/^/[node$i] /" &
    i=$(( i + 1 ))
done

wait
