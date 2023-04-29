#!/bin/sh

set -eu

usage() {
    echo "Usage: $0 <num servers> [-- [args...]]"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

set -x

N=$1

pids=
trap 'kill $pids 2>/dev/null' INT QUIT TERM EXIT

while [ "$#" -ge 1 -a "${1:?x}" != '--' ]; do
    shift
done
if [ "$#" -ge 1 ]; then
    shift
fi

i=0
while [ "$i" -lt "$N" ]; do
    go run ./cmd/dotsserver -config server_conf.yml -node_id "node$(( i + 1 ))" "$@" &
    pids="$pids $!"
    i=$(( i + 1 ))
done

wait
