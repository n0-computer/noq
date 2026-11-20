# Sourced by server.sh and client.sh, with IFACE and USAGE set. Checks the interface and that
# noq-perf can use AF_XDP without root. Changes nothing.
# shellcheck shell=bash

PATH=$PATH:/usr/sbin:/sbin
die() { echo "error: $*" >&2; exit 1; }
run() { echo "+ $*"; "$@"; }

[ -n "$IFACE" ] || die "$USAGE"
[ -e "/sys/class/net/$IFACE" ] || die "no interface $IFACE (see 'ip -br addr')"
[ "$(cat "/sys/class/net/$IFACE/operstate")" = up ] || die "$IFACE is not up"
MY_MAC=$(cat "/sys/class/net/$IFACE/address")
MY_CIDRS=$(ip -o -4 addr show dev "$IFACE" | awk '{print $4}')
QUEUES=$(find "/sys/class/net/$IFACE/queues" -maxdepth 1 -name 'rx-*' | wc -l)
echo "this machine: $IFACE, ip $(xargs <<<"$MY_CIDRS"), mac $MY_MAC, $QUEUES rx queues"

NOQ_PERF=${NOQ_PERF:-$(dirname "$0")/../../target/release/noq-perf}
[ -x "$NOQ_PERF" ] || die "no noq-perf at $NOQ_PERF: build it (perf/AF_XDP.md) or set NOQ_PERF"
[[ $("$NOQ_PERF" server --help) == *--xdp-neighbor* ]] ||
  die "$NOQ_PERF lacks AF_XDP or is outdated: rebuild with --features af-xdp"
# AF_XDP needs these capabilities; the binary gets them once after each build (perf/AF_XDP.md).
[[ ,$(findmnt -no OPTIONS -T "$NOQ_PERF"), != *,nosuid,* ]] ||
  die "$NOQ_PERF is on a nosuid mount, which ignores file capabilities: copy it elsewhere and set NOQ_PERF"
caps=$(getcap "$NOQ_PERF")
caps=${caps#"$NOQ_PERF" }
[[ $caps == *cap_net_admin* && $caps == *cap_net_raw* && $caps == *cap_ipc_lock* &&
  ($caps == *cap_bpf* || $caps == *cap_sys_admin*) && $caps == *[=+]ep* ]] ||
  die "$NOQ_PERF lacks the capabilities for AF_XDP (has: ${caps:-none}); run once after each build:
  sudo setcap cap_net_admin,cap_net_raw,cap_bpf,cap_ipc_lock+ep $(realpath "$NOQ_PERF")"
