#!/usr/bin/env bash
# Client side of a UDP vs AF_XDP comparison between two machines on one LAN; start server.sh
# on the other machine first. See perf/AF_XDP.md, "Two machines".
#
# Runs the noq-perf client over kernel UDP (to port 4433), then over AF_XDP (to port 4434)
# with the same arguments, and prints both results side by side.
# Changes: a permanent neighbor entry for <server-ip>, removed on exit; the XDP program during
# the AF_XDP run, detached by the kernel when noq-perf exits.
# ATTACH=generic avoids the native attach's NIC ring reset, but is slow.
set -euo pipefail
USAGE="usage: sudo $0 <iface> <server-ip> <server-mac> [duration] [noq-perf client args...]"
IFACE=${1:-} PEER_IP=${2:-} PEER_MAC=${3:-}
DUR=${4:-10}
ATTACH=${ATTACH:-native}
[[ $DUR =~ ^[0-9]+$ ]] || { echo "error: duration must be seconds, got $DUR" >&2; exit 1; }
# shellcheck source=perf/lan/common.sh
source "$(dirname "$0")/common.sh"
shift $(($# < 4 ? 3 : 4))

OUT=$(mktemp -d /tmp/noq-lan.XXXXXX)
ARGS=(--local-addr "$LOCAL_IP:4435" --duration "$DUR" "$@")

echo "== UDP run, $DUR s"
run "$NOQ_PERF" client "$PEER_IP:4433" "${ARGS[@]}" 2>&1 | tee "$OUT/udp.txt"
echo "== AF_XDP run, $DUR s, $ATTACH attach, $QUEUES queues"
sleep 3 # the server attaches XDP; the client's handshake retries until it is ready
run "$NOQ_PERF" client "$PEER_IP:4434" "${ARGS[@]}" --xdp "$IFACE" --xdp-queues "$QUEUES" \
  --xdp-attach "$ATTACH" --xdp-gateway "$PEER_MAC" 2>&1 | tee "$OUT/xdp.txt"

echo "== Summary, $DUR s each, arguments: ${*:-none}; full output in $OUT"
echo "    $(grep -m1 'Upload Duration' "$OUT/udp.txt")"
for r in udp xdp; do echo "$r $(grep ' AVG ' "$OUT/$r.txt" | tail -n1)"; done
for r in udp xdp; do echo "$r $(grep 'RPS:' "$OUT/$r.txt" | tail -n1)"; done
echo "Last AF_XDP counters of this machine:"
grep 'AF_XDP queue' "$OUT/xdp.txt" | tail -n2 || echo "(none, the run was shorter than 5 s)"
