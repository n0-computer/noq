#!/usr/bin/env bash
# Removes what server.sh or client.sh may have left behind if they were killed, and what the
# older xdp-enable.sh changed. Safe to run any number of times. See perf/AF_XDP.md.
# Usage: sudo perf/lan/cleanup.sh <iface> [peer-ip]
set -euo pipefail
PATH=$PATH:/usr/sbin:/sbin
IFACE=${1:?usage: sudo perf/lan/cleanup.sh <iface> [peer-ip]}
PEER_IP=${2:-}
run() { echo "+ $*"; "$@"; }
[ -e "/sys/class/net/$IFACE" ] || { echo "error: no interface $IFACE" >&2; exit 1; }
[ "$(id -u)" = 0 ] || { echo "error: run with sudo" >&2; exit 1; }

# The neighbor entry that server.sh and client.sh pin for the peer
if [ -n "$PEER_IP" ] && [ -n "$(ip neigh show "$PEER_IP" dev "$IFACE" nud permanent)" ]; then
  run ip neigh del "$PEER_IP" dev "$IFACE"
fi
echo "permanent neighbor entries on $IFACE: $(ip neigh show dev "$IFACE" nud permanent | xargs)"

# noq-perf's XDP program is detached by the kernel when noq-perf exits; only remove it if
# it is noq's and still attached.
if [[ $(ip -d link show dev "$IFACE") == *"name noq_xdp"* ]]; then
  run ip link set dev "$IFACE" xdp off || echo "noq_xdp still attached: stop noq-perf ($(pgrep -a noq-perf | xargs))"
fi

# --- Legacy: undo the older perf/xdp-enable.sh. Delete this block once no machine needs it.
# Its flow rule at location 0: udp4 to an address of IFACE, port 4433 (server) or 4434 (client).
# ntuple stays on, as xdp-disable.sh left it; without rules it changes nothing.
rule=$(ethtool -n "$IFACE" rule 0 2>/dev/null || true)
dst=$(sed -n 's/.*Dest IP addr: \([0-9.]*\) .*/\1/p' <<<"$rule")
if grep -q 'UDP over IPv4' <<<"$rule" && grep -Eq 'Dest port: 443[34] ' <<<"$rule" &&
  [ -n "$dst" ] && [[ $(ip -o -4 addr show dev "$IFACE") == *" $dst/"* ]]; then
  run ethtool -N "$IFACE" delete 0
fi
# Its arp_ignore/arp_announce values, saved before it changed them
SAVED=/run/noq-xdp-$IFACE.sysctl
if [ -f "$SAVED" ]; then
  while read -r d ignore announce; do
    run sysctl -w "net/ipv4/conf/$d/arp_ignore=$ignore" "net/ipv4/conf/$d/arp_announce=$announce"
  done <"$SAVED"
  run rm "$SAVED"
fi
# --- End of legacy block
