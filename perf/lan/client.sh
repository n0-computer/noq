#!/usr/bin/env bash
# Client side of a UDP vs AF_XDP comparison between two machines on one LAN; start server.sh
# on the other machine first. See perf/AF_XDP.md, "Two machines".
#
# Runs the noq-perf client over kernel UDP (to port 4433), then over AF_XDP (to port 4434)
# with the same arguments, and prints both results side by side. Runs without root. Changes
# only the XDP program during the AF_XDP run, detached by the kernel when noq-perf exits.
# Without [server-mac], AF_XDP sends to the MAC the kernel resolved for <server-ip>.
# ATTACH=generic avoids the native attach's NIC ring reset, but is slow.
set -euo pipefail
USAGE="usage: $0 <iface> <server-ip> [server-mac] [duration] [noq-perf client args...]"
IFACE=${1:-} PEER_IP=${2:-}
ATTACH=${ATTACH:-native}
# shellcheck source=perf/lan/common.sh
source "$(dirname "$0")/common.sh"
[[ $PEER_IP =~ ^[0-9]+(\.[0-9]+){3}$ ]] || die "not an IPv4 address: '$PEER_IP'; $USAGE"
shift 2
PEER_MAC='' DUR=10
if [[ ${1:-} =~ ^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$ ]]; then PEER_MAC=${1,,}; shift; fi
if [[ ${1:-} =~ ^[0-9]+$ ]]; then DUR=$1; shift; fi
[ "$PEER_MAC" != "$MY_MAC" ] || die "$PEER_MAC is this machine's MAC; pass the server's"

# Both the kernel and noq send to the server from the interface the route uses.
route=$(ip -o route get "$PEER_IP") || die "no route to $PEER_IP"
[[ $route != local* ]] || die "$PEER_IP is an address of this machine"
[[ $route != *" via "* ]] || die "$PEER_IP is reached through a gateway, not on this LAN: $route"
dev=$(sed -n 's/.* dev \([^ ]*\).*/\1/p' <<<"$route")
[ "$dev" = "$IFACE" ] || die "the route to $PEER_IP goes via $dev, not $IFACE: $route"
LOCAL_IP=$(sed -n 's/.* src \([^ ]*\).*/\1/p' <<<"$route")

# The kernel resolves the server's MAC on IFACE; noq reads it from /proc/net/arp.
ping -c 1 -W 1 "$PEER_IP" >/dev/null || echo "note: no ping reply from $PEER_IP"
ARP_MAC=$(ip neigh show "$PEER_IP" dev "$IFACE" | sed -n 's/.* lladdr \([0-9a-f:]*\).*/\1/p')
if [ -n "$PEER_MAC" ]; then
  XDP_ARGS=(--xdp-neighbor "$PEER_IP=$PEER_MAC")
  echo "server: $PEER_IP, mac $PEER_MAC for AF_XDP (given), ${ARP_MAC:-none} for UDP (kernel ARP)"
else
  [ -n "$ARP_MAC" ] || die "no MAC for $PEER_IP on $IFACE ('ip neigh show $PEER_IP dev $IFACE'): is the server up? Or pass the MAC server.sh prints"
  XDP_ARGS=()
  echo "server: $PEER_IP, mac $ARP_MAC (kernel ARP)"
fi
echo "local address: $LOCAL_IP on $IFACE"

OUT=$(mktemp -d /tmp/noq-lan.XXXXXX)
ARGS=(--local-addr "$LOCAL_IP:4435" --duration "$DUR" "$@")

echo "== UDP run, $DUR s"
run "$NOQ_PERF" client "$PEER_IP:4433" "${ARGS[@]}" 2>&1 | tee "$OUT/udp.txt"
echo "== AF_XDP run, $DUR s, $ATTACH attach, $QUEUES queues"
sleep 3 # the server attaches XDP; the client's handshake retries until it is ready
run "$NOQ_PERF" client "$PEER_IP:4434" "${ARGS[@]}" --xdp "$IFACE" --xdp-queues "$QUEUES" \
  --xdp-attach "$ATTACH" "${XDP_ARGS[@]}" 2>&1 | tee "$OUT/xdp.txt" || true

echo "== Summary, $DUR s each, arguments: ${*:-none}; full output in $OUT"
echo "    $(grep -m1 'Upload Duration' "$OUT/udp.txt")"
for r in udp xdp; do echo "$r $(grep ' AVG ' "$OUT/$r.txt" | tail -n1)"; done
for r in udp xdp; do echo "$r $(grep 'RPS:' "$OUT/$r.txt" | tail -n1)"; done
echo "Last AF_XDP counters of this machine:"
grep 'AF_XDP queue' "$OUT/xdp.txt" | tail -n2 || echo "(none, the run was shorter than 5 s)"
if ! grep -q 'RPS:' "$OUT/xdp.txt"; then
  echo "The AF_XDP run failed; see its output above and the server's."
  [ -n "$PEER_MAC" ] || echo "If it got no replies (rx_packets 0, handshake timeout), the kernel may have \
learned another port's MAC for the server (ARP flux). Pass the MAC server.sh prints:
  $0 $IFACE $PEER_IP <server-mac> $DUR $*"
  exit 1
fi
