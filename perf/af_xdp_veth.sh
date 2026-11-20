#!/bin/sh
# Compares noq-perf over UDP and over AF_XDP on a veth pair between two network namespaces.
#
# Needs root (or CAP_NET_ADMIN, CAP_NET_RAW, CAP_BPF and CAP_SYS_ADMIN for `ip netns`).
# See AF_XDP.md for details.
#
# Usage: af_xdp_veth.sh [path/to/noq-perf]
# Environment: DUR (seconds per run, default 10), QUEUES (veth queues, default 1),
#              CLIENT_ARGS (extra client arguments), RUST_LOG.
set -eu

PERF=${1:-noq-perf}
DUR=${DUR:-10}
QUEUES=${QUEUES:-1}
CLIENT_ARGS=${CLIENT_ARGS:-}
export RUST_LOG="${RUST_LOG:-warn}"

cleanup() {
  ip netns del noq-srv 2>/dev/null || true
  ip netns del noq-cli 2>/dev/null || true
}
trap cleanup EXIT
cleanup

ip netns add noq-srv
ip netns add noq-cli
# Deleting a namespace deletes the veth end inside it, and with it the pair.
ip link add xa netns noq-srv numrxqueues "$QUEUES" numtxqueues "$QUEUES" type veth \
  peer name xb netns noq-cli numrxqueues "$QUEUES" numtxqueues "$QUEUES"
ip -n noq-srv addr add 10.0.0.1/24 dev xa
ip -n noq-cli addr add 10.0.0.2/24 dev xb
for ns in noq-srv noq-cli; do ip -n $ns link set lo up; done
ip -n noq-srv link set xa up
ip -n noq-cli link set xb up
# Fills the ARP tables, which the AF_XDP sockets read before they learn MACs from traffic.
ip netns exec noq-cli ping -c1 -W1 10.0.0.1 >/dev/null

run() {
  name=$1 server_args=$2 client_args=$3
  echo "=== $name"
  # shellcheck disable=SC2086
  ip netns exec noq-srv "$PERF" server --listen 10.0.0.1:4433 $server_args &
  server=$!
  sleep 1
  # shellcheck disable=SC2086
  ip netns exec noq-cli "$PERF" client 10.0.0.1:4433 --local-addr 10.0.0.2:4434 \
    --duration "$DUR" $client_args $CLIENT_ARGS || true
  kill "$server"
  wait "$server" 2>/dev/null || true
}

run udp "" ""
run udp-multi-socket "--multi-socket" "--multi-socket"
run xdp "--xdp xa --xdp-queues $QUEUES" "--xdp xb --xdp-queues $QUEUES"
