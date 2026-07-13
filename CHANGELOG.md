# Changelog

All notable changes to noq will be documented in this file.

## [1.1.1](https://github.com/n0-computer/noq/compare/noq-v1.0.1..1.1.1) - 2026-07-13

### ⛰️  Features

- *(noq)* Introduce noq::Connection::authenticated - ([3e926fa](https://github.com/n0-computer/noq/commit/3e926fa9d40ffbc5267efbbcbcf953f3a0388860))

### 📚 Documentation

- *(noq)* Expand RecvStream::is_0rtt docs - ([497973c](https://github.com/n0-computer/noq/commit/497973c8604e3a1e4163ba4a403c443c7e301557))
- *(noq)* Document SendStream::stopped for detecting 0-RTT rejection - ([7e2de91](https://github.com/n0-computer/noq/commit/7e2de9179e5198b37e2769f4f9fb6cbfe9c14536))

### 🧪 Testing

- *(noq)* Improve 0-RTT integration test - ([190ef12](https://github.com/n0-computer/noq/commit/190ef12009ffa9b9ec4fe499b24b84269d76de27))

### ⚙️ Miscellaneous Tasks

- Update for rust 1.97 ([#745](https://github.com/n0-computer/noq/issues/745)) - ([21eea63](https://github.com/n0-computer/noq/commit/21eea63fbce3ea20d1eff49abcfa15c246f3fb82))
- Improve semver checks for stable releases ([#734](https://github.com/n0-computer/noq/issues/734)) - ([9663986](https://github.com/n0-computer/noq/commit/966398681f861861714fed52fecf1c4467fc968f))

## [noq-v1.0.1](https://github.com/n0-computer/noq/compare/noq-v1.0.0..noq-v1.0.1) - 2026-06-29

### 🐛 Bug Fixes

- *(proto)* Avoid `active_connections` underflow ([#717](https://github.com/n0-computer/noq/issues/717)) - ([32467e4](https://github.com/n0-computer/noq/commit/32467e456724dcbc0dd882e7133db48ec6f471c0))

### ⚙️ Miscellaneous Tasks

- Release - ([340e9c7](https://github.com/n0-computer/noq/commit/340e9c7da0d60eda6f5c7ffa7a36d20ed8d793fd))

## [noq-v1.0.0](https://github.com/n0-computer/noq/compare/noq-v1.0.0-rc.1..noq-v1.0.0) - 2026-06-15

### ⚙️ Miscellaneous Tasks

- Release - ([88b0546](https://github.com/n0-computer/noq/commit/88b05460ce23985aa34f271c03f1b6c9db29a909))

## [noq-v1.0.0-rc.1](https://github.com/n0-computer/noq/compare/noq-v1.0.0-rc.0..noq-v1.0.0-rc.1) - 2026-05-26

### ⛰️  Features

- *(proto)* Mark `PathEvent` as `#[non_exhaustive]` ([#648](https://github.com/n0-computer/noq/issues/648)) - ([be30bc5](https://github.com/n0-computer/noq/commit/be30bc5e2423475787974eb57d329ecb13566992))
- Add `Endpoint::wait_all_draining` to enable faster endpoint closing ([#651](https://github.com/n0-computer/noq/issues/651)) - ([269e5e0](https://github.com/n0-computer/noq/commit/269e5e0c38b62631d9a2f72ae236c6e3b91ad93d))

### 🚜 Refactor

- *(multipath)* [**breaking**] Rename PathEvent::Opened to Established ([#644](https://github.com/n0-computer/noq/issues/644)) - ([6a114f5](https://github.com/n0-computer/noq/commit/6a114f5a7f423ca9f48408afd068c3ecc952b546))
- *(noq)* [**breaking**] Use FourTuple in open_path ([#661](https://github.com/n0-computer/noq/issues/661)) - ([8188014](https://github.com/n0-computer/noq/commit/8188014dbdb9586f8d963c56a52a5f1ee2f31630))

### ⚙️ Miscellaneous Tasks

- Check external types in CI ([#643](https://github.com/n0-computer/noq/issues/643)) - ([684c3e2](https://github.com/n0-computer/noq/commit/684c3e25317f210ab20e159a49b0c93661990556))
- Release - ([c80da25](https://github.com/n0-computer/noq/commit/c80da2500637b9057710b06597ce6e215e41d50f))

## [noq-v1.0.0-rc.0](https://github.com/n0-computer/noq/compare/noq-v0.18.0..noq-v1.0.0-rc.0) - 2026-05-07

### ⛰️  Features

- *(noq)* [**breaking**] Return Closed struct from Connection::on_closed with path stats ([#617](https://github.com/n0-computer/noq/issues/617)) - ([3fc2e28](https://github.com/n0-computer/noq/commit/3fc2e28f9e82719a4a425aa33053b34e6842eca9))
- *(proto)* [**breaking**] Rename NAT traversal config and expose multipath default value ([#621](https://github.com/n0-computer/noq/issues/621)) - ([e25d7dd](https://github.com/n0-computer/noq/commit/e25d7dd60a277162680c1bc2d0fd0d6dc826a24b))
- Make negotiated_key_exchange_group always available ([#633](https://github.com/n0-computer/noq/issues/633)) - ([fe19376](https://github.com/n0-computer/noq/commit/fe19376f80022fd1880218cf7cbd5a712cab482f))

### 🚜 Refactor

- *(noq)* Atomic path ref counts ([#626](https://github.com/n0-computer/noq/issues/626)) - ([c64cf98](https://github.com/n0-computer/noq/commit/c64cf9840071049463b9a0256d5f49fb87c7c2b7))
- [**breaking**] Cleanup single path based expectations ([#616](https://github.com/n0-computer/noq/issues/616)) - ([fd36bc5](https://github.com/n0-computer/noq/commit/fd36bc5bf5f6779e744add3d2e6364e20e0af559))
- [**breaking**] Return previous path status from Path::set_status ([#638](https://github.com/n0-computer/noq/issues/638)) - ([1facdd9](https://github.com/n0-computer/noq/commit/1facdd9a44bc06bc18d9d990f6b1838b3a02811a))
- Rename write_chunk to write_bytes and write_chunks to write_bytes_many ([#536](https://github.com/n0-computer/noq/issues/536)) - ([f4ec777](https://github.com/n0-computer/noq/commit/f4ec7775afedb35b3c4a39228f424883ebe6c74a))
- Rename read_chunk to read_bytes and make it return just a Bytes ([#535](https://github.com/n0-computer/noq/issues/535)) - ([a0f988a](https://github.com/n0-computer/noq/commit/a0f988a91db325be0a2ce7e65536016af881951b))

### 📚 Documentation

- *(quinn)* Improve `Connection::close_reason()` documentation - ([1fdd690](https://github.com/n0-computer/noq/commit/1fdd6904476cd46fd14bbe1cefdcd3f819ea048c))

### ⚙️ Miscellaneous Tasks

- Sync with quinn@main ([#606](https://github.com/n0-computer/noq/issues/606)) - ([877dcca](https://github.com/n0-computer/noq/commit/877dcca064416b7d14701573833c4767ab468005))
- Reexport all public noq-proto types at noq level ([#615](https://github.com/n0-computer/noq/issues/615)) - ([ecd08ae](https://github.com/n0-computer/noq/commit/ecd08ae5f53f00fdf559b8643281219c77e95aac))
- Change deps to be more explicit - ([307adcd](https://github.com/n0-computer/noq/commit/307adcd7864fbde951928533216269418bc43e30))
- Release - ([6ee7cf2](https://github.com/n0-computer/noq/commit/6ee7cf2f8cbd17a941b8c639351ad9a09451cbc1))

### Quinn

- Move ConnectionRef/EndpointRef ref counts onto Shared as AtomicUsize - ([c1d7ed2](https://github.com/n0-computer/noq/commit/c1d7ed2734beb44018799407bb0825259a934bb2))
- Make Endpoint::server dual-stack V6 by default - ([ef2be07](https://github.com/n0-computer/noq/commit/ef2be0709d3c120a456f6bc467e8da860277e353))

## [noq-v0.18.0](https://github.com/n0-computer/noq/compare/v0.17.0..noq-v0.18.0) - 2026-04-15

### ⛰️  Features

- *(noq)* Unify waking the state ([#541](https://github.com/n0-computer/noq/issues/541)) - ([5fd0ad1](https://github.com/n0-computer/noq/commit/5fd0ad1276b6023f01945be752901f9407b6a8d5))

### 🐛 Bug Fixes

- *(proto)* [**breaking**] Ensure network paths have cleaned socket addresses ([#513](https://github.com/n0-computer/noq/issues/513)) - ([b3b50c0](https://github.com/n0-computer/noq/commit/b3b50c0b492734e903e3f2fab761d908dab6d1c4))
- *(proto)* Re-arm PathIdle timer when idle timeout is changed ([#544](https://github.com/n0-computer/noq/issues/544)) - ([cdde4e9](https://github.com/n0-computer/noq/commit/cdde4e92f34b73f2ac63c249a29510844c13616f))
- *(proto)* Reset PTO backoff for recoverable paths on network change ([#545](https://github.com/n0-computer/noq/issues/545)) - ([b41b57a](https://github.com/n0-computer/noq/commit/b41b57a11ef10300764d3862b8aa4f5a049ebc9a))
- *(udp)* Propagate network-unreachable send errors instead of swallowing them ([#527](https://github.com/n0-computer/noq/issues/527)) - ([04eecd5](https://github.com/n0-computer/noq/commit/04eecd59a8b5886d560a34af8b54b64417da0561))
- Ensure hole punching related frames don't get stuck ([#540](https://github.com/n0-computer/noq/issues/540)) - ([b236d7d](https://github.com/n0-computer/noq/commit/b236d7d89ae1f0318f761297131ff8b99f491e08))

### 🚜 Refactor

- *(proto)* Move FrameStats into PathStats ([#521](https://github.com/n0-computer/noq/issues/521)) - ([fd8e5ba](https://github.com/n0-computer/noq/commit/fd8e5bafe04788bbc969302ad65ce36ed02501ef))
- Remove poll_read_buf from public api ([#548](https://github.com/n0-computer/noq/issues/548)) - ([c9d9bf3](https://github.com/n0-computer/noq/commit/c9d9bf35092d9291e0917b387141979bfeff88c6))

### ⚙️ Miscellaneous Tasks

- *(docs)* Check internal docs as well ([#499](https://github.com/n0-computer/noq/issues/499)) - ([a92084b](https://github.com/n0-computer/noq/commit/a92084ba7a90a218b92716f6418e65b8928a1bd7))
- *(proto)* Update to rand 0.10 ([#511](https://github.com/n0-computer/noq/issues/511)) - ([1280ffd](https://github.com/n0-computer/noq/commit/1280ffd01bb1f031a61a5e8ea1a09f4e0baed466))
- Fix release config - ([6b17679](https://github.com/n0-computer/noq/commit/6b1767964daab5b8bc26933f222819612339fa4b))
- Add more cargo-make targets and update CI template ([#586](https://github.com/n0-computer/noq/issues/586)) - ([de242d8](https://github.com/n0-computer/noq/commit/de242d85f48178d411afa9160f101da30b2adef7))
- Fix release config - ([bbcae8e](https://github.com/n0-computer/noq/commit/bbcae8e0ab0d7747b770ba4fd633589ba0173574))
- Release - ([6933db9](https://github.com/n0-computer/noq/commit/6933db95b09db3f08318fdc642e00946cb6282a0))

### Deps

- Hide the tokio streams behind simple newtype wrappers ([#547](https://github.com/n0-computer/noq/issues/547)) - ([b212bbc](https://github.com/n0-computer/noq/commit/b212bbcaccaa82089cc17fb29c4458d113a0cae6))

## [0.17.0](https://github.com/n0-computer/noq/compare/iroh-quinn-v0.16.1..v0.17.0) - 2026-03-09

### ⛰️  Features

- [**breaking**] Allow compiling with `rustls`, but without any crypto providers compiled into rustls ([#462](https://github.com/n0-computer/noq/issues/462)) - ([13a1c45](https://github.com/n0-computer/noq/commit/13a1c456f543cd9d8bfa58ca3b0ea890a678efb4))

### 🐛 Bug Fixes

- Avoid lock re-entry in open_path_ensure and add regression test ([#464](https://github.com/n0-computer/noq/issues/464)) - ([6c4de85](https://github.com/n0-computer/noq/commit/6c4de8557ef48ec1eb24dcdbb0a720d7da43fb7e))

### 🚜 Refactor

- [**breaking**] Rename to noq ([#461](https://github.com/n0-computer/noq/issues/461)) - ([294e3ea](https://github.com/n0-computer/noq/commit/294e3ea603a2c8f86d05c486082b96f1175ad5e5))

### ⚙️ Miscellaneous Tasks

- Attempt at drafting a readme ([#467](https://github.com/n0-computer/noq/issues/467)) - ([e71b78b](https://github.com/n0-computer/noq/commit/e71b78b88a9d44eee6d7aaa61f1ad2b603fbf6d4))
- Release prep - ([13695a4](https://github.com/n0-computer/noq/commit/13695a47ab1d0c151c536e0f3e5c07b80b315c44))
- Release - ([faeddf5](https://github.com/n0-computer/noq/commit/faeddf58eed8b9b30a153aed5d9acee570934837))


