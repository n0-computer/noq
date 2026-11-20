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

AF_XDP needs Linux 5.9 or later (the program is attached through a BPF link) and these
capabilities:

| What noq-perf does | Capability |
|---|---|
| Create the XSKMAP (`BPF_MAP_CREATE`) | CAP_BPF, and CAP_NET_ADMIN for this map type |
| Load the XDP program (`BPF_PROG_LOAD`) | CAP_BPF, and CAP_NET_ADMIN for networking programs |
| Attach it to the interface (`BPF_LINK_CREATE`) | CAP_NET_ADMIN |
| Create the AF_XDP socket | CAP_NET_RAW |
| Register the UMEM (`XDP_UMEM_REG`) | CAP_IPC_LOCK, or an RLIMIT_MEMLOCK (`ulimit -l`) above the UMEM size: up to 16 MiB per socket by default (`--xdp-frames`), more than the common 8 MiB limit |
| Fill the XSKMAP, map the rings, bind, read `XDP_STATISTICS` | none beyond the above: they act on the file descriptors created above |

Running as root works. Without root, give the binary these capabilities once after each build:

```sh
sudo setcap cap_net_admin,cap_net_raw,cap_bpf,cap_ipc_lock+ep target/release/noq-perf
```

- Every rebuild replaces the file and drops the capabilities; run `setcap` again.
- Anyone who can execute the binary gets these capabilities. Restrict it, for example with
  `chmod 700` (or `chmod 750` and a group).
- File capabilities are ignored on a `nosuid` mount (often `/tmp`); `findmnt -no OPTIONS -T
  <binary>` shows it. Copy the binary elsewhere then.
- Kernels before 5.8 have no CAP_BPF; there `cap_sys_admin` replaces `cap_bpf` (but AF_XDP
  here needs 5.9 anyway).
- The process is not dumpable, so attaching `gdb` or `perf record -p` to it needs root.

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
| `--xdp-neighbor <ip>=<mac>` | MAC to send to for `<ip>`, before the kernel's ARP table; may be repeated |
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
They run without root and change no NIC settings, sysctls, addresses, routes or neighbor
entries.

### Steps

1. Build on both machines, and give the binary its capabilities (see "Privileges"; again
   after every build). The scripts use `target/release/noq-perf` of the checkout they are in;
   for another binary, set `NOQ_PERF=/path/to/noq-perf`.

   ```sh
   cargo build --release -p perf --features af-xdp
   sudo setcap cap_net_admin,cap_net_raw,cap_bpf,cap_ipc_lock+ep target/release/noq-perf
   ```

   Both scripts check that the binary has the capabilities and is not on a `nosuid` mount,
   and print the `setcap` command otherwise.

2. Start the server with the interface facing the client. It listens on the interface's
   IPv4 address (pass `[ip]` after the interface if it has several), prints its MAC and the
   command for the client:

   ```sh
   # storage1
   perf/lan/server.sh enp67s0f0
   # this machine: enp67s0f0, ip 192.168.1.59/24, mac 50:7c:6f:16:22:38, 32 rx queues
   # on the client run: perf/lan/client.sh <iface> 192.168.1.59 [duration] [args...]
   ```

3. Run the client with its interface facing the server (`[duration]` in seconds, default
   10; further arguments go to both client runs):

   ```sh
   # compute1
   perf/lan/client.sh eno1 192.168.1.59 10 --download-size 100M --upload-size 0
   ```

   The client runs over kernel UDP first, then over AF_XDP with the same arguments, and ends
   with a summary of both. The server serves exactly these two connections and exits; start
   both again for another round. The outputs are kept in `/tmp/noq-lan.*` on the client.

Before running anything, the scripts check that the interface exists and is up and that
`noq-perf` was built with AF_XDP and has its capabilities. The client also checks that `ip
route get <server-ip>` goes directly (no gateway) via its interface, and takes its local
address from that route.

### What changes on your system

- Nothing persistent, except the `setcap`'d binary (and `/tmp/noq-lan.*` on the client).
- While noq-perf runs over AF_XDP, its XDP program `noq_xdp` on the interface; the kernel
  detaches it when noq-perf exits, even if it is killed.

`perf/lan/cleanup.sh <iface>` reports a `noq_xdp` program that is still attached (it is
only while its noq-perf process runs). It also undoes what the older `xdp-enable.sh` changed,
which needs sudo: it deletes the flow rule at location 0 if it steers UDP to an address of
`<iface>` at port 4433 or 4434, and restores `arp_ignore`/`arp_announce` from
`/run/noq-xdp-<iface>.sysctl` if that file exists. It leaves ntuple on, as `xdp-disable.sh`
did; without rules it has no effect.

### How the destination MAC is found

noq sends AF_XDP frames to the MAC it learned from frames received from that IP address,
else to a `--xdp-neighbor <ip>=<mac>` entry, else to the kernel's ARP entry for that IP on
the interface (`/proc/net/arp`), else to `--xdp-gateway`. A learned MAC replaces a given one.

- The server needs no MAC: it only replies, to the address and MAC of the client's frames.
- The client pings the server once, so that the kernel resolves its MAC on the interface, and
  prints the MAC from `ip neigh show <server-ip> dev <iface>`, which noq then reads from
  `/proc/net/arp`.

ARP flux: if the server has several ports in one subnet, by default any of them may answer
ARP for the server's address, and the client may learn the MAC of a port without the XDP
program. The AF_XDP run then gets no replies. `server.sh` warns when another of its
interfaces has an address in the subnet, and prints the client command with its MAC:

```sh
perf/lan/client.sh eno1 192.168.1.59 50:7c:6f:16:22:38 10 --download-size 100M --upload-size 0
```

With the MAC, the client's AF_XDP run uses `--xdp-neighbor <server-ip>=<server-mac>`, which
takes precedence over the kernel's ARP entry (`--xdp-gateway` would not). The UDP run still
uses the kernel's ARP entry: its packets may arrive on the other port, which the kernel
accepts, so it works, but may use the other link. If that matters for the comparison, use
one port per subnet on the server.

### How the comparison is kept fair

- Neither machine has an XDP program attached during the UDP run: the server serves the UDP
  connection on port 4433 (`noq-perf server --once`), exits, and only then attaches XDP and
  serves the AF_XDP connection on port 4434. The client attaches its own only for the
  AF_XDP run. The client waits 3 seconds between the runs, and its handshake retries until the
  server is ready.
- Both runs use the same addresses, ports on the NICs (but see ARP flux above) and client
  arguments.
- The AF_XDP endpoint is an endpoint with multiple sockets, the UDP one is not; pass
  `--multi-socket` to both scripts (last on the server, after the duration on the client)
  to rule that out. With `--xdp` it has no effect.

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
after the duration on the client, last on the server). The UMEM is locked memory, which
CAP_IPC_LOCK allows without limit.

### Attach mode

The scripts attach XDP in native mode (driver mode), which gives representative numbers. On
some drivers, including ixgbe and i40e, attaching or detaching a native XDP program
reconfigures the NIC's rings, which briefly interrupts all traffic on that port, at the start
and end of the AF_XDP run. `ATTACH=generic perf/lan/...` on both machines uses
generic mode, which does not touch the driver, but is slow and not representative.

Notes:

- Drivers with native XDP and AF_XDP zero-copy include i40e, ice, ixgbe, mlx5 and igc (`ethtool
  -i <iface>`). Others fall back to copy mode, and drivers without native XDP (r8169, e1000e,
  Wi-Fi) fail with native mode. The scripts log `zero_copy=true|false` at
  `RUST_LOG=noq::xdp=info` (`RUST_LOG=noq::xdp=info perf/lan/...`).
- Many drivers refuse XDP with LRO on or with an MTU above about 3498 (single-buffer XDP).
- In zero-copy mode, packets for the kernel are copied out of the UMEM during the AF_XDP run,
  so other traffic on the port is slightly slower but works.

### Troubleshooting

The `AF_XDP queue N: XdpStats { .. }` lines on stderr (every 5 seconds) show what goes wrong.

| Symptom | Meaning |
|---|---|
| A script says the binary lacks the capabilities | Run the `setcap` command it prints; needed again after every build. |
| A script says the binary is on a `nosuid` mount | File capabilities are ignored there: copy the binary elsewhere and set `NOQ_PERF`. |
| `creating XSKMAP: Operation not permitted` | The capabilities are missing or ignored, see "Privileges". |
| The client finds no MAC for the server | The server did not answer ARP on that interface: is it up, is the address right? Or pass the MAC `server.sh` prints. |
| The client's AF_XDP run gets no replies (handshake timeout, `rx_packets` 0 on every queue), the UDP run worked | Usually ARP flux: the client sends to another port of the server. Pass the MAC `server.sh` prints as the third argument of `client.sh`. Otherwise a firewall drops the packets. |
| `tx_no_neighbor` grows | No MAC for the peer: not learned, no ARP entry, no `--xdp-neighbor` or `--xdp-gateway`. |
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
- No routing: the destination MAC comes from received frames, `--xdp-neighbor`, the kernel's
  ARP table, or `--xdp-gateway`. A client that talks first needs one of the last three.
- No GSO or GRO; transmits are split into frames in user space, and checksums are computed in
  software. Received checksums are not verified (QUIC authenticates every packet).
- Each socket has its own UMEM (no `XDP_SHARED_UMEM`), and with several queues only the first
  socket sends.
- The XDP program redirects only IPv4 UDP packets without IP options to the configured
  address and port; everything else, including ARP and ICMP, goes to the kernel stack.
