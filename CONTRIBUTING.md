# Contributing

Thanks for your interest in contributing to noq! noq is a QUIC implementation
forked from [Quinn](https://github.com/quinn-rs/quinn), with added support for
multipath QUIC, Address discovery and NAT traversal.

## Workspace Layout

| Crate | Purpose |
|---|---|
| `noq` | Async user-facing API (tokio or smol) |
| `noq-proto` | Sans-io protocol state machine — no I/O, no clocks |
| `noq-udp` | Low-level UDP with ECN, GRO/GSO, cmsg |
| `bench` | Criterion benchmarks |
| `perf` | `noq-perf` binary for throughput/latency profiling |
| `fuzz` | libfuzzer targets |
| `docs/book` | mdBook source |

## Architecture

The codebase has a deliberate two-tier split:

- **`noq-proto`** is a pure state machine: deterministic, testable in
  isolation, no sockets, no internal clock driving the state machine — time is
  always passed in by the caller. Feed it packets and events; it returns frames
  to transmit and state changes.
- **`noq`** wraps `noq-proto` with async I/O, timers, and a runtime
  abstraction. Public `noq` types re-export everything from `noq-proto` so
  callers rarely to depend on `noq-proto` directly.

## Code contributions!

### A great pull request to noq has:

- An issue linked to it. Discuss solutions with maintainers in the linked issue
  before diving in. This helps keep contributors and maintainers aligned.
- A tittle following this pattern: `<type>(<scope>): <description>`. If the
  change is a breaking change it must also include a `!`: `<type>(<scope>)!: <description>`.
  - Types: 
       | **`type`** | **When to use** |
       |--:         |-- |
       | `feat`     | A new feature |
       | `test`     | Changes that exclusively affect tests, either by adding new ones or correcting existing ones |
       | `fix`      | A bug fix |
       | `docs`     | Documentation only changes |
       | `refactor` | A code change that neither fixes a bug nor adds a feature |
       | `perf`     | A code change that improves performance |
       | `deps`     | Dependency only updates |
       | `chore`    | Changes to the build process or auxiliary tools and libraries |
  - Scopes: These mostly mimic the crate. You will likely use one of `noq`,
    `proto`, `udp`
  - Description: A short sentence stating what the changes achieve
- A PR description. Please follow the PR template
- A green CI check. Use `cargo make` to catch most issues locally.

### Formatting, linting and docs

- Documentation updates follow the [style guide](https://rust-lang.github.io/rfcs/1574-more-api-documentation-conventions.html#appendix-a-full-conventions-text).
- Keep comments and docs at 100 character width.
- Run `cargo make format` to ensure your code follows the formatting rules of
  this repo.

## For maintainers

### Syncing quinn changes

- Assuming `upstream` is the local name you gave the quinn remote, use `git
  fetch upstream main` to get the latest changes.
- Use `git log --oneline --reverse HEAD..upstream/main`, while on updated
  `main` to check the missing changes. The first change is the oldest one/
  first incoming change `main` does not have
- Use `cherry-pick` selectively. Dependabot updates should not be
  cherry-picked, as they need to be fully replicated via the corresponding
  `cargo update -p <dep> --precise <version>`. Git can't meaningfully apply
  these commits only via `diff` since `Cargo.lock` is automatically generated.
- Always use `cherry-pick` with `-xe`. Use `-x` to ensure the original commit
  hash is included in the commit message at the end. Use `-e` to adapt the
  commit message to our commits standards. This is necessary to generate a
  meaningful `CHANGELOG.md`
- Verify CI status for individual changes. Not everything can be caught via
  `cargo make`, for example due to different targets.

### Recording quinn merges in git

When all changes are done, let's call `<quinn-hash>` the last commit from
`quinn` we want to record as merged. Let's call `<noq-hash>` the last commit
that achieves this merged state, before the actual merge is recorded. Make sure
your syncing branch is not behind main, as this merge will be fast-forwards
one. In your syncing branch `HEAD` should point to `<noq-hash>`. Now do

- `git merge <quinn-hash>` to begin the merge process
- `git checkout <noq-hash> .` To update all paths to the state in `<noq-hash>`
- Make sure there are no differences with `<noq-hash>`, you might need to `git
  rm -r quinn-proto` and similar to achieve this. Then `git diff <noq-hash>`
  should be clean.
- `git merge --continue` to finish the merge. Let's call `<merge-hash>` the
  resulting commit.
- `git push`. Always verify against CI that the merge commit is good to go.
- After CI gives you green light, locally merge to `noq`'s main using `git
  merge --ff-only <merge-hash>`
