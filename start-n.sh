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

pids=
trap 'kill $pids 2>/dev/null' INT QUIT TERM EXIT

while [ "$#" -ge 1 ] && [ "$1" != '--' ]; do
    shift
done
if [ "$#" -ge 1 ]; then
    shift
fi

i=$first
while [ "$i" -le "$last" ]; do
    go run ./cmd/dotsserver -config server_conf.yml -node_id "node$i" -listen_offset "$i" "$@" &
    pids="$pids $!"
    i=$(( i + 1 ))
done

wait
