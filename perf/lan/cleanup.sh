#!/usr/bin/env bash
# server.sh and client.sh leave nothing behind: the kernel detaches noq-perf's XDP program when
# noq-perf exits. This script reports a noq_xdp program that is still attached, and undoes what
# the older xdp-enable.sh changed. It needs sudo only if it finds something to undo.
# Safe to run any number of times. See perf/AF_XDP.md.
# Usage: perf/lan/cleanup.sh <iface>
set -euo pipefail
PATH=$PATH:/usr/sbin:/sbin
IFACE=${1:?usage: perf/lan/cleanup.sh <iface>}
die() { echo "error: $*" >&2; exit 1; }
as_root() { [ "$(id -u)" = 0 ] || die "run with sudo to: $*"; echo "+ $*"; "$@"; }
[ -e "/sys/class/net/$IFACE" ] || die "no interface $IFACE"

# A noq_xdp program attached through a BPF link stays only while its process runs.
if [[ $(ip -d link show dev "$IFACE") == *"name noq_xdp"* ]]; then
  echo "noq_xdp is attached to $IFACE; running noq-perf: $(pgrep -a noq-perf | xargs)"
  as_root ip link set dev "$IFACE" xdp off || die "could not detach it: stop noq-perf"
fi

# --- Legacy, needs sudo: undo the older perf/xdp-enable.sh. Delete once no machine needs it.
# Its flow rule at location 0: udp4 to an address of IFACE, port 4433 (server) or 4434 (client).
# ntuple stays on, as xdp-disable.sh left it; without rules it changes nothing.
rule=$(ethtool -n "$IFACE" rule 0 2>/dev/null || true)
dst=$(sed -n 's/.*Dest IP addr: \([0-9.]*\) .*/\1/p' <<<"$rule")
if grep -q 'UDP over IPv4' <<<"$rule" && grep -Eq 'Dest port: 443[34] ' <<<"$rule" &&
  [ -n "$dst" ] && [[ $(ip -o -4 addr show dev "$IFACE") == *" $dst/"* ]]; then
  as_root ethtool -N "$IFACE" delete 0
fi
# Its arp_ignore/arp_announce values, saved before it changed them
SAVED=/run/noq-xdp-$IFACE.sysctl
if [ -f "$SAVED" ]; then
  while read -r d ignore announce; do
    as_root sysctl -w "net/ipv4/conf/$d/arp_ignore=$ignore" "net/ipv4/conf/$d/arp_announce=$announce"
  done <"$SAVED"
  as_root rm "$SAVED"
fi
# --- End of legacy block
echo "$IFACE: nothing (more) to undo"
