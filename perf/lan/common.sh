# Sourced by server.sh and client.sh, with IFACE, PEER_IP and PEER_MAC set.
# Checks the inputs, then pins PEER_MAC for PEER_IP on IFACE until the script exits.
# Nothing is changed before all checks pass.
# shellcheck shell=bash

PATH=$PATH:/usr/sbin:/sbin
die() { echo "error: $*" >&2; exit 1; }
run() { echo "+ $*"; "$@"; }

[ -n "$IFACE" ] || die "$USAGE"
[ -e "/sys/class/net/$IFACE" ] || die "no interface $IFACE (see 'ip -br addr')"
[ "$(cat "/sys/class/net/$IFACE/operstate")" = up ] || die "$IFACE is not up"
MY_MAC=$(cat "/sys/class/net/$IFACE/address")
QUEUES=$(find "/sys/class/net/$IFACE/queues" -maxdepth 1 -name 'rx-*' | wc -l)
echo "this machine: $IFACE, ip $(ip -o -4 addr show dev "$IFACE" | awk '{print $4}' | xargs), mac $MY_MAC, $QUEUES rx queues"
[[ -n $PEER_IP && -n $PEER_MAC ]] || die "$USAGE"

[[ $PEER_IP =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "not an IPv4 address: $PEER_IP"
PEER_MAC=${PEER_MAC,,}
[[ $PEER_MAC =~ ^([0-9a-f]{2}:){5}[0-9a-f]{2}$ ]] || die "not a MAC address: $PEER_MAC"
[ "$PEER_MAC" != "$MY_MAC" ] || die "$PEER_MAC is this machine's MAC; pass the other machine's"

# Both the kernel and noq send to the peer from the interface the route uses.
route=$(ip -o route get "$PEER_IP") || die "no route to $PEER_IP"
[[ $route != local* ]] || die "$PEER_IP is an address of this machine"
[[ $route != *" via "* ]] || die "$PEER_IP is reached through a gateway, not on this LAN: $route"
dev=$(sed -n 's/.* dev \([^ ]*\).*/\1/p' <<<"$route")
[ "$dev" = "$IFACE" ] || die "the route to $PEER_IP goes via $dev, not $IFACE: $route"
LOCAL_IP=$(sed -n 's/.* src \([^ ]*\).*/\1/p' <<<"$route")
echo "peer: $PEER_IP, mac $PEER_MAC, reached from $LOCAL_IP on $IFACE"

NOQ_PERF=${NOQ_PERF:-$(dirname "$0")/../../target/release/noq-perf}
[ -x "$NOQ_PERF" ] || die "no noq-perf at $NOQ_PERF: build it (perf/AF_XDP.md) or set NOQ_PERF"
[[ $("$NOQ_PERF" server --help) == *--xdp-frames* ]] || die "$NOQ_PERF lacks AF_XDP or is outdated: rebuild with --features af-xdp"
[ "$(id -u)" = 0 ] || die "run with sudo: the neighbor entry and AF_XDP need root"

# Traffic from this machine to PEER_IP goes to PEER_MAC, also if another port of the peer
# answers ARP for PEER_IP. Nothing else is affected.
unpin() { run ip neigh del "$PEER_IP" dev "$IFACE" || true; }
trap unpin EXIT
trap 'exit 130' INT TERM
run ip neigh replace "$PEER_IP" lladdr "$PEER_MAC" dev "$IFACE" nud permanent
