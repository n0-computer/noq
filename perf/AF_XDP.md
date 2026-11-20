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
  UMEM. Each socket has 16 MiB of UMEM by default, more than the common 8 MiB limit.

Running as root is simplest. Alternatively grant the capabilities to the binary:

```sh
sudo setcap cap_bpf,cap_net_admin,cap_net_raw+ep target/release/noq-perf
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
| `--multi-socket` | Without `--xdp`: put the UDP socket in an endpoint with multiple sockets |

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

## Two machines with real NICs

1. Pick an interface with an IPv4 address on a shared link, for example `eth0` with
   10.0.0.1 on the server and 10.0.0.2 on the client.
2. Make sure the QUIC traffic arrives on the queues the sockets are bound to. Either reduce
   the NIC to one queue:

   ```sh
   ethtool -L eth0 combined 1
   ```

   or steer the flow to queue 0 and keep the other queues for other traffic:

   ```sh
   ethtool -N eth0 flow-type udp4 dst-ip 10.0.0.1 dst-port 4433 action 0
   ```

   or bind all queues with `--xdp-queues $(ethtool -l eth0 | awk '/Combined/{print $2}' | tail -1)`.
   With several queues, all sockets receive but the first one sends everything.
3. Fill the neighbor tables (`ping -c1 <peer>`), or pass `--xdp-gateway <peer MAC>` if the
   peer is reached through a router.
4. Run the server and the client as in the veth example, once with `--xdp eth0` and once
   without, and compare.

Check `ethtool -i eth0` for the driver; drivers with native XDP and AF_XDP zero-copy include
i40e, ice, ixgbe, mlx5 and igc. `--xdp-bind zerocopy` fails if the driver does not support
it; the server logs `zero_copy=true|false` at `RUST_LOG=noq::xdp=info`.

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
