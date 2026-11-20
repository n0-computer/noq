#!/usr/bin/env bash
# Server side of a UDP vs AF_XDP comparison between two machines on one LAN; client.sh runs
# on the other machine. See perf/AF_XDP.md, "Two machines".
#
# Serves one connection over kernel UDP on port 4433, then attaches XDP and serves one over
# AF_XDP on port 4434, so the UDP run does not pass through an XDP program. Runs without
# root. Changes only the XDP program while the AF_XDP server runs, detached by the kernel when
# it exits. [ip] is needed only if <iface> has several IPv4 addresses.
# ATTACH=generic avoids the native attach's NIC ring reset, but is slow.
set -euo pipefail
USAGE="usage: $0 <iface> [ip] [noq-perf server args...]"
IFACE=${1:-}
ATTACH=${ATTACH:-native}
# shellcheck source=perf/lan/common.sh
source "$(dirname "$0")/common.sh"
shift

[ -n "$MY_CIDRS" ] || die "$IFACE has no IPv4 address"
if [[ ${1:-} =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then
  CIDR=$(awk -F/ -v ip="$1" '$1 == ip' <<<"$MY_CIDRS")
  [ -n "$CIDR" ] || die "$1 is not an address of $IFACE"
  shift
else
  [ "$(wc -l <<<"$MY_CIDRS")" = 1 ] || die "$IFACE has several IPv4 addresses, pass one; $USAGE"
  CIDR=$MY_CIDRS
fi
IP=${CIDR%/*}

# ARP flux: if another port of this machine is in the same subnet, the kernel may answer ARP
# for IP from that port, and the client then sends to the wrong port.
ip2int() { local IFS=.; read -r a b c d <<<"$1"; echo $(((a << 24) + (b << 16) + (c << 8) + d)); }
mask=$(((0xffffffff << (32 - ${CIDR#*/})) & 0xffffffff))
net=$(($(ip2int "$IP") & mask))
others=$(ip -o -4 addr show | while read -r _ dev _ cidr _; do
  n=$(ip2int "${cidr%/*}")
  if [ "$dev" != "$IFACE" ] && (((n & mask) == net)); then
    echo -n "$dev (${cidr%/*}) "
  fi
done)
if [ -n "$others" ]; then
  echo "warning: other ports in the same subnet: ${others% }; the client may learn the wrong MAC (ARP flux)"
  echo "on the client run: perf/lan/client.sh <iface> $IP $MY_MAC [duration] [args...]"
else
  echo "on the client run: perf/lan/client.sh <iface> $IP [duration] [args...]"
  echo "  (the client learns this machine's MAC, $MY_MAC, through ARP)"
fi

echo "== UDP server on $IP:4433, waiting for client.sh"
run "$NOQ_PERF" server --once --listen "$IP:4433" "$@"
echo "== AF_XDP server on $IP:4434, $ATTACH attach, $QUEUES queues"
run "$NOQ_PERF" server --once --listen "$IP:4434" --xdp "$IFACE" --xdp-queues "$QUEUES" \
  --xdp-attach "$ATTACH" "$@"
