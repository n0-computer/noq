# Measuring noq over AF_XDP

`noq-perf` can run its client and server over AF_XDP sockets (`noq::xdp`, behind the noq
feature `af-xdp`) instead of a UDP socket. This file explains how to build it, which
privileges it needs, how to set up a test network, and what to compare.

## Build

```sh
cargo build --release -p perf --features af-xdp
```

The binary is `noq-perf` in the target directory (`cargo metadata --format-version 1
--no-deps | jq -r .target_directory`, then `release/noq-perf`).

## Privileges

AF_XDP needs Linux 5.9 or later and:

- CAP_BPF and CAP_NET_ADMIN (or CAP_SYS_ADMIN) to load the XDP program and its XSKMAP;
- CAP_NET_ADMIN to attach the program to the interface;
- CAP_NET_RAW to create the AF_XDP socket;
- CAP_IPC_LOCK, or an RLIMIT_MEMLOCK (`ulimit -l`) above the UMEM size, to register the
  UMEM. Each socket has up to 16 MiB of UMEM by default (`--xdp-frames`), more than the
  common 8 MiB limit.

Running as root is simplest. Alternatively grant the capabilities to the binary:

```sh
sudo setcap cap_bpf,cap_net_admin,cap_net_raw,cap_ipc_lock+ep target/release/noq-perf
```

`ip netns` in the scripts below needs root.

The program is attached through a BPF link: it is detached when the process exits, even if
it is killed. Only one XDP program can be attached to an interface this way; if `ip link
show <dev>` already shows one (`prog/xdp`), remove it first.

## Command line

AF_XDP options, on both `server` and `client`:

| Option | Meaning |
|---|---|
| `--xdp <dev>` | Use AF_XDP on interface `<dev>` |
| `--xdp-queues <n>` | Bind one AF_XDP socket to each of the queues `0..n` (default 1) |
| `--xdp-attach auto\|generic\|native` | XDP attach mode (default `auto`: native if the driver supports it) |
| `--xdp-bind auto\|copy\|zerocopy` | Zero-copy or copy mode (default `auto`: zero-copy if supported) |
| `--xdp-gateway <mac>` | MAC to send to if a destination's MAC is unknown |
| `--xdp-segments <n>` | Datagrams per transmit, the emulated GSO batch (default 32) |
| `--xdp-frames <n>` | 4 KiB UMEM frames per socket (default 4096, fewer with many queues: at most 256 MiB in total) |
| `--multi-socket` | Without `--xdp`: put the UDP socket in an endpoint with multiple sockets |

`noq-perf server --once` exits after its first connection ends.

With `--xdp`, the local address must be a concrete IPv4 address of the interface with a
port: `--listen 10.0.0.1:4433` on the server, `--local-addr 10.0.0.2:4434` on the client.

The AF_XDP endpoint is always an endpoint with multiple sockets
(`Endpoint::new_with_multiple_sockets`), while plain UDP uses a single-socket endpoint. Run
the UDP baseline with and without `--multi-socket` to see whether the endpoint type matters.

Every 5 seconds the AF_XDP sockets print their counters to stderr (`XdpStats`): packets
received and sent, datagrams dropped for lack of a neighbor MAC or for exceeding the MTU,
sends that had to wait for TX space, and the kernel's `XDP_STATISTICS`. Non-zero
`tx_no_neighbor`, `kernel_rx_ring_full` or `kernel_rx_fill_ring_empty` mean the numbers are
not trustworthy.

## Test network: veth pair in two namespaces

`perf/af_xdp_veth.sh` creates a veth pair between the namespaces `noq-srv` (10.0.0.1, `xa`)
and `noq-cli` (10.0.0.2, `xb`), and runs three client/server pairs: UDP, UDP with
`--multi-socket`, and AF_XDP on both ends.

```sh
sudo DUR=10 perf/af_xdp_veth.sh "$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)/release/noq-perf"
```

The equivalent manual setup:

```sh
ip netns add noq-srv
ip netns add noq-cli
ip link add xa netns noq-srv type veth peer name xb netns noq-cli
ip -n noq-srv addr add 10.0.0.1/24 dev xa
ip -n noq-cli addr add 10.0.0.2/24 dev xb
ip -n noq-srv link set xa up
ip -n noq-cli link set xb up
ip netns exec noq-cli ping -c1 10.0.0.1     # fills the ARP tables

ip netns exec noq-srv noq-perf server --listen 10.0.0.1:4433 --xdp xa &
ip netns exec noq-cli noq-perf client 10.0.0.1:4433 --local-addr 10.0.0.2:4434 --xdp xb --duration 10
```

Without root on the host, a privileged container works too, for example:

```sh
TARGET=$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)
docker run --rm --privileged -v "$TARGET/release:/opt/bin:ro" -v "$PWD/perf:/perf:ro" \
  -e DUR=10 archlinux:latest sh /perf/af_xdp_veth.sh /opt/bin/noq-perf
```

Notes on veth:

- veth supports native XDP, but not zero-copy, so AF_XDP on veth is always copy mode.
- Create the pair with `numrxqueues N numtxqueues N` (the script does this with
  `QUEUES=N`) to test several queues. A single flow still lands on one queue.
- Both ends run on one host, so the client and server compete for the same CPUs. veth
  numbers show that the path works and give a rough idea of per-packet cost; they are not a
  substitute for two machines.

## Two machines on one LAN

`perf/lan/` has three short scripts that compare kernel UDP with AF_XDP between two machines.
They change no NIC settings, sysctls, addresses or routes; see below for what they do change.

### Steps

1. Build on both machines. The scripts use `target/release/noq-perf` of the checkout they
   are in; for another binary, run them as `sudo NOQ_PERF=/path/to/noq-perf perf/lan/...`.

   ```sh
   cargo build --release -p perf --features af-xdp
   ```

2. Pick the interface on each machine that the route to the other machine uses. Running a
   script with only the interface prints this machine's address and MAC, to copy to the other
   machine:

   ```sh
   perf/lan/server.sh enp67s0f0
   # this machine: enp67s0f0, ip 192.168.1.59/24, mac 50:7c:6f:16:22:38, 32 rx queues
   ```

3. Start the server, then the client (`[duration]` in seconds, default 10; further arguments
   go to both client runs):

   ```sh
   # storage1
   sudo perf/lan/server.sh enp67s0f0 192.168.1.39 3c:ec:ef:30:47:6c
   # compute1
   sudo perf/lan/client.sh eno1 192.168.1.59 50:7c:6f:16:22:38 10 --download-size 100M --upload-size 0
   ```

   The client runs over kernel UDP first, then over AF_XDP with the same arguments, and ends
   with a summary of both. The server serves exactly these two connections and exits; start
   both again for another round. The outputs are kept in `/tmp/noq-lan.*` on the client.

4. If a script was killed (`kill -9`, lost SSH session), clean up on that machine:

   ```sh
   sudo perf/lan/cleanup.sh <iface> <peer-ip>
   ```

Before changing anything, each script checks that the interface exists and is up, that
`ip route get <peer-ip>` goes directly (no gateway) via that interface, that the MAC is well
formed and not this machine's own, and that `noq-perf` exists and was built with AF_XDP. It
takes the local address from the route. Every command that changes the system is printed with
a `+` before it runs.

### What the scripts change, and how it is reverted

| Change | Effect | Reverted by |
|---|---|---|
| `ip neigh replace <peer-ip> lladdr <peer-mac> dev <iface> nud permanent` | Traffic from this machine to `<peer-ip>` goes to `<peer-mac>`; nothing else is affected | The script on exit, also on Ctrl-C (`ip neigh del <peer-ip> dev <iface>`); `cleanup.sh` |
| The XDP program `noq_xdp` on `<iface>`, only during the AF_XDP run | Every packet on the port passes through the program; QUIC packets to noq-perf's port go to noq, all others to the kernel as usual | The kernel, when noq-perf exits (it is attached through a BPF link); `cleanup.sh` |
| `/tmp/noq-lan.*` on the client | The output of both runs | Delete it when done |

`cleanup.sh` also undoes what the older `xdp-enable.sh` changed: it deletes the flow rule at
location 0 if it steers UDP to an address of `<iface>` at port 4433 or 4434, and restores
`arp_ignore`/`arp_announce` from `/run/noq-xdp-<iface>.sysctl` if that file exists. It leaves
ntuple on, as `xdp-disable.sh` did; without rules it has no effect.

Why the neighbor entry: when two ports of a machine are in one subnet, either may answer ARP
for the other's address (ARP flux), and the peer then sends to the wrong port, where no XDP
socket listens. Each side chooses the destination MAC of its own sends, so pinning the peer's
MAC on both machines makes both directions use the right ports, for kernel UDP and AF_XDP
alike. A permanent entry is not overwritten by ARP replies. noq reads it from
`/proc/net/arp`; `--xdp-gateway <peer-mac>` is passed as well, as a fallback. Pass the MAC that
the other machine's script prints, not one from `ip neigh`, which may be the wrong port's.

### How the comparison is kept fair

- Neither machine has an XDP program attached during the UDP run: the server serves the UDP
  connection on port 4433 (`noq-perf server --once`), exits, and only then attaches XDP and
  serves the AF_XDP connection on port 4434. The client attaches its own only for the
  AF_XDP run. The client waits 3 seconds between the runs, and its handshake retries until the
  server is ready.
- Both runs use the same addresses, MACs, ports on the NICs and client arguments.
- The AF_XDP endpoint is an endpoint with multiple sockets, the UDP one is not; pass
  `--multi-socket` to both scripts (after the MAC on the server, after the duration on the
  client) to rule that out. With `--xdp` it has no effect.

### No flow steering: a socket on every queue

The scripts do not configure the NIC (no `ethtool -N` rules, no channel changes). Instead
noq-perf binds an AF_XDP socket on every receive queue (`--xdp-queues <number of rx queues>`),
so whichever queue RSS picks for the flow is covered. All sockets receive; the one on queue 0
sends. Only sockets that have seen traffic print their counters.

Each socket has its own UMEM. By default noq-perf gives each socket 4096 frames of 4 KiB
(16 MiB), but at most 256 MiB for all sockets together: 2048 frames (8 MiB) each with 32
queues, 1040 frames (about 4 MiB) each with 63, about 256 MiB in total. Half of a socket's
frames receive, half send. Fewer frames mean less buffering on the queue that carries the
flow; if `kernel_rx_fill_ring_empty` or `tx_blocked` grows, raise `--xdp-frames` (passed
after the duration on the client, after the MAC on the server). The UMEM is locked memory,
which root may use without limit.

### Attach mode

The scripts attach XDP in native mode (driver mode), which gives representative numbers. On
some drivers, including ixgbe and i40e, attaching or detaching a native XDP program
reconfigures the NIC's rings, which briefly interrupts all traffic on that port, at the start
and end of the AF_XDP run. `sudo ATTACH=generic perf/lan/...` on both machines uses
generic mode, which does not touch the driver, but is slow and not representative.

Notes:

- Drivers with native XDP and AF_XDP zero-copy include i40e, ice, ixgbe, mlx5 and igc (`ethtool
  -i <iface>`). Others fall back to copy mode, and drivers without native XDP (r8169, e1000e,
  Wi-Fi) fail with native mode. The scripts log `zero_copy=true|false` at
  `RUST_LOG=noq::xdp=info` (`sudo RUST_LOG=noq::xdp=info perf/lan/...`).
- Many drivers refuse XDP with LRO on or with an MTU above about 3498 (single-buffer XDP).
- In zero-copy mode, packets for the kernel are copied out of the UMEM during the AF_XDP run,
  so other traffic on the port is slightly slower but works.

### Troubleshooting

The `AF_XDP queue N: XdpStats { .. }` lines on stderr (every 5 seconds) show what goes wrong.

| Symptom | Meaning |
|---|---|
| `tx_no_neighbor` grows | No MAC for the peer. Should not happen with the scripts (they pin it and pass `--xdp-gateway`); without them, ping the peer or pass `--xdp-gateway <MAC>`. |
| `tx_packets` grows, `rx_packets` stays 0 on every queue | Nothing reaches the sockets: the peer sends to another port's MAC (check that the MAC given to the peer's script is this machine's on `<iface>`), the peer is not running, or a firewall drops the packets. |
| `rx_packets` grows but the run stalls | The peer receives nothing: check the same on the other machine. |
| `kernel_rx_fill_ring_empty` grows | The kernel had no free frame to receive into, packets are dropped: raise `--xdp-frames`, or the receiver does not keep up. |
| `kernel_rx_ring_full` grows | The RX ring was full: the application does not read fast enough. |
| `kernel_rx_dropped` grows | The kernel dropped frames for other reasons, for example frames larger than the UMEM frame. |
| `tx_too_big` grows | Datagrams larger than the interface MTU, usually MTU probes; harmless in small numbers. |
| `tx_blocked` grows | Sends waited for TX frames or ring slots: the NIC is the bottleneck, or raise `--xdp-frames`. |
| Attaching fails | Another XDP program is attached (`ip -d link show <iface>`), LRO is on, the MTU is too large, or the driver has no native XDP (try `ATTACH=generic`). |
| The client's AF_XDP run hangs in the handshake | The server's AF_XDP server did not start; see its output. |

## What to measure

- Throughput and requests per second, from the perf client's output. For bulk throughput use
  `--download-size 100M --upload-size 0` (or the reverse); for small-request rates use
  `--download-size 1K --upload-size 1K --bi-requests 16`.
- CPU per byte: run under `perf stat -e task-clock` or watch `pidstat -t 1`; AF_XDP should
  pay off as less system time per packet, which matters more than peak throughput on veth.
- Packet rate: compare `tx_packets` from the AF_XDP counters with the interface counters
  (`ip -s link show <dev>`, `ethtool -S <dev>`).
- Copy versus zero-copy, generic versus native attach mode, and `--xdp-segments 1` versus 32
  (the effect of batching frames per syscall).
- The `--json <file>` client output, to collect the runs.

## Limitations

- IPv4 only. Every socket sends from the one configured address; transmits to IPv6 are
  dropped and counted (`tx_not_ipv4`).
- No routing: the destination MAC comes from received frames, the kernel's ARP table, or
  `--xdp-gateway`. A client that talks first needs the ARP entry or the gateway option.
- No GSO or GRO; transmits are split into frames in user space, and checksums are computed in
  software. Received checksums are not verified (QUIC authenticates every packet).
- Each socket has its own UMEM (no `XDP_SHARED_UMEM`), and with several queues only the first
  socket sends.
- The XDP program redirects only IPv4 UDP packets without IP options to the configured
  address and port; everything else, including ARP and ICMP, goes to the kernel stack.
