#!/usr/bin/env bash
# shellcheck disable=SC2015  # ok, warn and fail never fail, so "test && ok || fail" is safe
# Read-only checks before running noq-perf over AF_XDP between two machines. Needs no root.
# Usage: perf/xdp-check.sh <iface> <peer-ip>
# See AF_XDP.md, "Two machines with real NICs".
set -euo pipefail
IFACE=${1:?usage: xdp-check.sh <iface> <peer-ip>}
PEER=${2:?usage: xdp-check.sh <iface> <peer-ip>}

ok() { echo "OK    $*"; }
warn() { echo "WARN  $*"; }
fail() { echo "FAIL  $*"; FAILED=1; }
FAILED=0

[ -e "/sys/class/net/$IFACE" ] || { fail "no interface $IFACE"; exit 1; }
state=$(cat "/sys/class/net/$IFACE/operstate")
[ "$state" = up ] && ok "$IFACE is up" || fail "$IFACE is $state"

# The kernel only resolves the peer's MAC on the interface the route uses, and noq reads it
# from there; another interface means every send is dropped as tx_no_neighbor.
route=$(ip -o route get "$PEER")
dev=$(echo "$route" | sed -n 's/.* dev \([^ ]*\).*/\1/p')
LOCAL_IP=$(echo "$route" | sed -n 's/.* src \([^ ]*\).*/\1/p')
via=$(echo "$route" | sed -n 's/.* via \([^ ]*\).*/\1/p')
[ "$dev" = "$IFACE" ] && ok "route to $PEER goes via $IFACE, local address $LOCAL_IP" ||
  fail "route to $PEER goes via $dev, not $IFACE: use --xdp $dev"
[ -z "$via" ] || warn "$PEER is routed via $via; pass --xdp-gateway <MAC of $via>"
NEXT=${via:-$PEER}

# Neighbor entry, as noq reads it: /proc/net/arp, device == iface, flags != 0 (complete)
ping -c1 -W1 "$NEXT" >/dev/null 2>&1 || warn "no ping reply from $NEXT"
MAC=$(awk -v ip="$NEXT" -v dev="$IFACE" '$1 == ip && $6 == dev && $3 != "0x0" {print $4}' /proc/net/arp)
[ -n "$MAC" ] && ok "neighbor $NEXT on $IFACE: $MAC" || fail "no ARP entry for $NEXT on $IFACE"
awk -v ip="$NEXT" -v dev="$IFACE" '$1 == ip && $6 != dev && $3 != "0x0" {print "WARN  " ip " is also in the ARP table on " $6 " (" $4 ")"}' /proc/net/arp

# Other interfaces in the same subnet: with arp_ignore=0 they answer ARP for this address too
# (ARP flux), and the peer may send to the wrong port.
net=$(ip -o -4 route show dev "$IFACE" proto kernel | grep -E " src $LOCAL_IP( |$)" | cut -d' ' -f1 || true)
subnet=$(ip -o -4 route show proto kernel | awk -v net="$net" '$1 == net {print $3}' | sort -u)
for d in $subnet; do
  echo "      $d: arp_ignore=$(cat "/proc/sys/net/ipv4/conf/$d/arp_ignore") arp_announce=$(cat "/proc/sys/net/ipv4/conf/$d/arp_announce")"
done
[ "$(echo "$subnet" | wc -w)" -le 1 ] || warn "several interfaces in $net (ARP flux risk): run xdp-enable.sh on both machines"
echo "      on the peer, 'ip neigh show $LOCAL_IP' must show $(cat "/sys/class/net/$IFACE/address")"

# NIC: driver, queues, flow steering, MTU, attached XDP program
echo "      $(ethtool -i "$IFACE" 2>/dev/null | head -n1)"
QUEUES=$(find "/sys/class/net/$IFACE/queues" -maxdepth 1 -name 'rx-*' | wc -l)
echo "      $QUEUES receive queues"
ntuple=$(ethtool -k "$IFACE" 2>/dev/null | sed -n 's/^ntuple-filters: //p')
echo "      ntuple-filters: ${ntuple:-not reported}"
ethtool -n "$IFACE" 2>/dev/null | grep -E 'Filter|Dest IP|Dest port|Action' | sed 's/^/      /' || true
mtu=$(cat "/sys/class/net/$IFACE/mtu")
[ "$mtu" -le 3498 ] && ok "MTU $mtu" || warn "MTU $mtu: single-buffer XDP needs 3498 or less on most drivers"
prog=$(ip -d link show dev "$IFACE" | grep -o 'prog/xdp.*' || true)
[ -z "$prog" ] && ok "no XDP program attached" || warn "XDP program attached: $prog (noq's is named noq_xdp)"

# With a steering rule for this address (xdp-enable.sh), one socket on queue 0 is enough;
# otherwise bind every queue.
if ethtool -n "$IFACE" 2>/dev/null | grep -q "Dest IP addr: $LOCAL_IP "; then XQ=1; else XQ=$QUEUES; fi
[ "$XQ" = 1 ] || warn "no steering rule for $LOCAL_IP: binding all $XQ queues (xdp-enable.sh adds one if ntuple is supported)"

BIN=${NOQ_PERF:-noq-perf}
ARGS="--xdp $IFACE --xdp-queues $XQ${MAC:+ --xdp-gateway $MAC}"
echo
echo "This machine as server: sudo $BIN server --listen $LOCAL_IP:4433 $ARGS"
echo "This machine as client: sudo $BIN client $PEER:4433 --local-addr $LOCAL_IP:4434 $ARGS"
[ "$FAILED" = 0 ] || { echo "Fix the FAILs first."; exit 1; }
