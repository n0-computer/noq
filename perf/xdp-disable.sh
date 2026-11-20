#!/usr/bin/env bash
# Reverts xdp-enable.sh. Needs root. Each step tolerates being already reverted.
# Usage: perf/xdp-disable.sh <iface>    (LOC as given to xdp-enable.sh, default 0)
# ntuple is left on: turning it off would also delete rules that existed before.
set -uo pipefail
IFACE=${1:?usage: xdp-disable.sh <iface>}
LOC=${LOC:-0}
SAVED=/run/noq-xdp-$IFACE.sysctl

run() { echo "+ $*"; "$@"; }
[ "$(id -u)" = 0 ] || { echo "needs root"; exit 1; }

run ethtool -N "$IFACE" delete "$LOC" || true
if [ -f "$SAVED" ]; then
  while read -r d ignore announce; do
    run sysctl -w "net/ipv4/conf/$d/arp_ignore=$ignore" "net/ipv4/conf/$d/arp_announce=$announce" || true
  done <"$SAVED"
  run rm "$SAVED"
else
  echo "no $SAVED: arp_ignore/arp_announce left as they are"
fi
# noq-perf's XDP program detaches when it exits; this only removes one left behind.
if ip -d link show dev "$IFACE" | grep -q 'name noq_xdp'; then
  run ip link set dev "$IFACE" xdp off || echo "still attached: is noq-perf running?"
fi
