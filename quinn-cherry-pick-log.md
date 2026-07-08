# Quinn → noq cherry-pick log (quinn-integration branch)

Range covers 35 commits from `quinn-rs/quinn` (upstream). Matched against
`quinn-integration` HEAD `db4ae654c` as of 2026-07-03.

| # | Quinn commit | noq commit | Commit name | Notes |
|---|---|---|---|---|
| 1 | [`42b8d15ec`](https://github.com/quinn-rs/quinn/commit/42b8d15ec) | `8f8019266` | feat(proto): allow ProtectedHeader::decode to use a generic reference | |
| 2 | [`f5fdc42f7`](https://github.com/quinn-rs/quinn/commit/f5fdc42f7) | skipped | Update rustls-webpki to pass security audit | Change was already present in noq |
| 3 | [`96b3beb97`](https://github.com/quinn-rs/quinn/commit/96b3beb97) | `497973c86` | doc(noq): Expand RecvStream::is_0rtt docs | |
| 4 | [`958fe9d1d`](https://github.com/quinn-rs/quinn/commit/958fe9d1d) | `3e926fa9d` | feat(noq): Introduce noq::Connection::authenticated | |
| 5 | [`7fad1d8f2`](https://github.com/quinn-rs/quinn/commit/7fad1d8f2) | `7e2de9179` | docs(noq): Document SendStream::stopped for detecting 0-RTT rejection | |
| 6 | [`e8b44cdd4`](https://github.com/quinn-rs/quinn/commit/e8b44cdd4) | skipped | Remove ZeroRttAccepted future | This is a breaking change, not something we can bring to the code right now |
| 7 | [`8c479a50c`](https://github.com/quinn-rs/quinn/commit/8c479a50c) | `190ef1200` | test(noq): Improve 0-RTT integration test | |
| 8 | [`c9b40f109`](https://github.com/quinn-rs/quinn/commit/c9b40f109) | skipped | qlog: emit RTT values in milliseconds | Already present via #639 |
| 9 | [`41dce3142`](https://github.com/quinn-rs/quinn/commit/41dce3142) | `a6042549b` | deps: bump rcgen from 0.14.7 to 0.14.8 | |
| 10 | [`3c43d45bd`](https://github.com/quinn-rs/quinn/commit/3c43d45bd) | `4cfbafa4c` | fix(proto): congestion: avoid double-reducing CUBIC fast convergence | |
| 11 | [`bd0c3bfea`](https://github.com/quinn-rs/quinn/commit/bd0c3bfea) | `6039c9375` | fix(proto): congestion: preserve excess CUBIC cwnd increment | |
| 12 | [`995512ff7`](https://github.com/quinn-rs/quinn/commit/995512ff7) | `51d70431d` | deps: bump log to 0.4.30 | |
| 13 | [`b2f4d932a`](https://github.com/quinn-rs/quinn/commit/b2f4d932a) | `ab43c64ce` | deps: bump serde_json to 1.0.150 | |
| 14 | [`5c05d2100`](https://github.com/quinn-rs/quinn/commit/5c05d2100) | `ef21cf5bc` | fix: Patches for Redox targets | |
| 15 | [`c27821ce7`](https://github.com/quinn-rs/quinn/commit/c27821ce7) | `9a95dbd5c` | chore(noq-udp): Apply suggestion from @mxinden | |
| 16 | [`31f7f12f5`](https://github.com/quinn-rs/quinn/commit/31f7f12f5) | `f847bb0b9` | fix(proto): set loss detection timer on path validation failure | |
| 17 | [`89d9eaacc`](https://github.com/quinn-rs/quinn/commit/89d9eaacc) | `5b65b30af` | deps: bump log to 0.4.31 | |
| 18 | [`63d75122a`](https://github.com/quinn-rs/quinn/commit/63d75122a) | `d3ad06c0f` | deps: bump socket2 to 0.6.4 | |
| 19 | [`81582f48f`](https://github.com/quinn-rs/quinn/commit/81582f48f) | skipped | feat(udp): add IP_RECVERR / IPV6_RECVERR support (Linux/Android) | Rejected by request of @Frando |
| 20 | [`79d654070`](https://github.com/quinn-rs/quinn/commit/79d654070) | skipped | Upgrade to rand 0.10.1 | noq's `Cargo.toml` already pins `rand = "0.10"` and already uses `rand::RngExt` |
| 21 | [`cffd741da`](https://github.com/quinn-rs/quinn/commit/cffd741da) | skipped | Switch BBR RNG to PCG | Our bbr(3) already uses it |
| 22 | [`c09962c7b`](https://github.com/quinn-rs/quinn/commit/c09962c7b) | `ccfad4d19` | chore(proto): Apply suggestions from clippy 1.96 | |
| 23 | [`1e77dc0eb`](https://github.com/quinn-rs/quinn/commit/1e77dc0eb) | `90857a868` | style(proto): move Window below Dedup | |
| 24 | [`5751078a9`](https://github.com/quinn-rs/quinn/commit/5751078a9) | `4586890ef` | docs(proto): tweak Window docstrings | |
| 25 | [`d915ac170`](https://github.com/quinn-rs/quinn/commit/d915ac170) | skipped | Bump MSRV to 1.88 (for qlog -> serde_with) | We already had this |
| 26 | [`bfbf1b968`](https://github.com/quinn-rs/quinn/commit/bfbf1b968) | skipped | Upgrade to qlog 0.18 | noq depends on its own fork, `n0-qlog` |
| 27 | [`bb446ebb4`](https://github.com/quinn-rs/quinn/commit/bb446ebb4) | skipped | udp: avoid rebinding argument | Modifies code that depends on the skipped `IP_RECVERR` feature |
| 28 | [`0d3c3be35`](https://github.com/quinn-rs/quinn/commit/0d3c3be35) | `141181495` | docs(udp): clean up docstrings | |
| 29 | [`d9ddb8110`](https://github.com/quinn-rs/quinn/commit/d9ddb8110) | skipped | udp: move LinuxError down | This is part of `IP_RECVERR` which we skipped|
| 30 | [`4b354e22a`](https://github.com/quinn-rs/quinn/commit/4b354e22a) | `fc6b02ba7` | style(udp): move msghdr_x and IpTosTy definitions down | |
| 31 | [`3ca0ff137`](https://github.com/quinn-rs/quinn/commit/3ca0ff137) | `f14b1b452` | style(udp): move CMSG_LEN to cmsg::LEN | |
| 32 | [`35ba3c451`](https://github.com/quinn-rs/quinn/commit/35ba3c451) | `aa95f9792` | refactor(udp): extract linux module | |
| 33 | [`517028f4b`](https://github.com/quinn-rs/quinn/commit/517028f4b) | `db4ae654c` | refactor(udp): extract apple_fast module | Current `quinn-integration` HEAD. |
| 34 | [`5999e14cb`](https://github.com/quinn-rs/quinn/commit/5999e14cb) | skipped | udp: LinuxError style tweaks | Depends on `LinuxError` (see #19/#29) — not present in noq. Cascade skip; not yet reached/applicable. |
| 35 | [`bd017d2ba`](https://github.com/quinn-rs/quinn/commit/bd017d2ba) | skipped | udp: fix clippy suggestions for Windows code | Targets a `WSAENOPROTOOPT as i32 \|\| WSAEOPNOTSUPP as i32` pattern in `windows.rs`; noq's current code already expresses this as `matches!(.., Some(WinSock::WSAEOPNOTSUPP \| WinSock::WSAENOPROTOOPT))`, a different (already-clippy-clean) shape, so this specific diff doesn't apply. |

## Summary

- **27 / 35** landed as direct cherry-picks with a clear 1:1 noq commit.
- **8 / 35** skipped, falling into two groups:
  - Genuinely skipped/deferred: #2 (redundant lockfile bump), #6 (avoided breaking API removal), #8 (already fixed independently), #25/#26 (MSRV/qlog already ahead via noq's own fork), #20/#21 (rand/BBR-RNG already present via noq's independent fork/merge).
  - Cascade-skipped: #19 introduces `IP_RECVERR`/`LinuxError` support; the cherry-pick was attempted but aborted on conflicts (see reflog) and never landed. #27, #29, #34, #35 all patch code that only exists once #19 lands, so they were skipped in turn.
