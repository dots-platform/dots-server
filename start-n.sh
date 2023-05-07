#!/bin/sh

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
