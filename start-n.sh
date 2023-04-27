#!/bin/sh

set -eu

usage() {
    echo "Usage: $0 <num servers>"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

set -x

N=$1

pids=
trap 'kill $pids 2>/dev/null' INT QUIT TERM EXIT

i=0
while [ "$i" -lt "$N" ]; do
    go run ./cmd/dotsserver -config server_conf.yml -node_id "node$(( i + 1 ))" &
    pids="$pids $!"
    i=$(( i + 1 ))
done

wait
