#!/usr/bin/env bash
# Server side of a UDP vs AF_XDP comparison between two machines on one LAN; client.sh runs
# on the other machine. See perf/AF_XDP.md, "Two machines".
#
# Serves one connection over kernel UDP on port 4433, then attaches XDP and serves one over
# AF_XDP on port 4434, so the UDP run does not pass through an XDP program.
# Changes: a permanent neighbor entry for <client-ip>, removed on exit; the XDP program while
# the AF_XDP server runs, detached by the kernel when it exits.
# ATTACH=generic avoids the native attach's NIC ring reset, but is slow.
set -euo pipefail
USAGE="usage: sudo $0 <iface> <client-ip> <client-mac> [noq-perf server args...]"
IFACE=${1:-} PEER_IP=${2:-} PEER_MAC=${3:-}
ATTACH=${ATTACH:-native}
# shellcheck source=perf/lan/common.sh
source "$(dirname "$0")/common.sh"
shift 3

echo "== UDP server on $LOCAL_IP:4433, waiting for client.sh"
run "$NOQ_PERF" server --once --listen "$LOCAL_IP:4433" "$@"
echo "== AF_XDP server on $LOCAL_IP:4434, $ATTACH attach, $QUEUES queues"
run "$NOQ_PERF" server --once --listen "$LOCAL_IP:4434" --xdp "$IFACE" --xdp-queues "$QUEUES" \
  --xdp-attach "$ATTACH" --xdp-gateway "$PEER_MAC" "$@"
