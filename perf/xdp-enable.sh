#!/usr/bin/env bash
# Prepares one machine for noq-perf over AF_XDP; xdp-disable.sh reverts it. Needs root.
# Usage: perf/xdp-enable.sh <iface> <local-ip> [port] [queue]
#   port   this machine's noq-perf port: 4433 on the server (default), 4434 on the client
#   queue  receive queue to steer the flow to (default 0); run noq-perf with --xdp-queues queue+1
#   LOC    environment: ntuple rule location (default 0), which xdp-disable.sh deletes
#
# Changes:
#   - ntuple on, and a rule at location LOC steering udp4 to <local-ip>:<port> to <queue>;
#   - arp_ignore=1 and arp_announce=2 on every interface in the subnet of <iface>, so only
#     <iface> answers ARP for <local-ip> (the originals are saved in /run/noq-xdp-<iface>.sysctl);
#   - flushes the neighbor tables of those interfaces, so the peer's MAC is learned again.
set -euo pipefail
IFACE=${1:?usage: xdp-enable.sh <iface> <local-ip> [port] [queue]}
IP=${2:?usage: xdp-enable.sh <iface> <local-ip> [port] [queue]}
PORT=${3:-4433}
QUEUE=${4:-0}
LOC=${LOC:-0}
SAVED=/run/noq-xdp-$IFACE.sysctl

run() { echo "+ $*"; "$@"; }
[ "$(id -u)" = 0 ] || { echo "needs root"; exit 1; }
ip -o -4 addr show dev "$IFACE" | grep -q " $IP/" || { echo "$IP is not an address of $IFACE"; exit 1; }

XQ=$((QUEUE + 1))
if ethtool -k "$IFACE" | grep -q '^ntuple-filters: off \[fixed\]'; then
  XQ=$(find "/sys/class/net/$IFACE/queues" -maxdepth 1 -name 'rx-*' | wc -l)
  echo "$IFACE has no ntuple support, so no steering: bind every queue"
elif ethtool -n "$IFACE" | grep -A8 "^Filter: $LOC$" | grep -q "Dest IP addr: $IP "; then
  echo "rule $LOC for $IP already exists"
elif ethtool -n "$IFACE" | grep -q "^Filter: $LOC$"; then
  echo "another rule is at location $LOC; delete it or set LOC"; exit 1
else
  run ethtool -K "$IFACE" ntuple on
  run ethtool -N "$IFACE" flow-type udp4 dst-ip "$IP" dst-port "$PORT" action "$QUEUE" loc "$LOC"
fi

net=$(ip -o -4 route show dev "$IFACE" proto kernel | grep -E " src $IP( |$)" | cut -d' ' -f1)
for d in $(ip -o -4 route show proto kernel | awk -v net="$net" '$1 == net {print $3}' | sort -u); do
  conf=/proc/sys/net/ipv4/conf/$d
  # Saved once, so running enable twice keeps the original values.
  grep -q "^$d " "$SAVED" 2>/dev/null || echo "$d $(cat "$conf/arp_ignore") $(cat "$conf/arp_announce")" >>"$SAVED"
  # Per interface: the effective value is the maximum of "all" and the interface's.
  run sysctl -w "net/ipv4/conf/$d/arp_ignore=1" "net/ipv4/conf/$d/arp_announce=2"
  run ip neigh flush dev "$d"
done

echo "Done. Check with perf/xdp-check.sh $IFACE <peer-ip>, run noq-perf with --xdp-queues $XQ,"
echo "and revert with: sudo perf/xdp-disable.sh $IFACE"
