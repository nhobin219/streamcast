# Contributing

The gates in this repo are strict and mostly automated, so the fastest way to a merged
PR is to know what they check before you write anything.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Setup

```bash
just bootstrap          # uv sync + git hooks
just check              # lint + format-check + typecheck + tests, exactly what CI runs
```

You need [`uv`](https://docs.astral.sh/uv/) and [`just`](https://github.com/casey/just).
Nothing else — the test suite needs no network, no container and no credentials. Every
test binds port 0 on loopback and every log is a temp directory, and a change that breaks
that is a change to reject.

## Commits

Conventional Commits, enforced by a `commit-msg` hook (`scripts/check_commit_msg.py`).
Both lists are closed — an unknown scope is rejected, so add one to the script in the
same commit if you genuinely need it:

```
<type>(<scope>): <lowercase description, no trailing period, subject <= 72 chars>

types   feat fix refactor perf test docs build chore
scopes  ci client deps errors examples log protocol replay server spec stream
        subscriber
```

Write the body for someone reading `git log` in a year: what was wrong, why this fix and
not the obvious one, what it cost. The history here is used as documentation and it is
expected to carry reasoning, not a restatement of the diff.

## Style

`ruff` for lint and format at line length 88,
[`ty`](https://github.com/astral-sh/ty) for types, and one house rule the formatter does
not know: **a blank line after every compound-statement block.**
`python scripts/check_blank_lines.py --fix` applies it, and pre-commit runs it for you.

Comments carry the reasoning, not the mechanics. A comment explaining what the next line
does is noise; one explaining why the obvious alternative was rejected is the reason this
codebase is navigable. Match the density of the file you are editing.

## Tests

```bash
just test               # everything
just test-fast          # skips the slow tier, for an inner loop
just test tests/test_resume.py -k partition
```

**Falsify every test you write.** Break the code the test covers and confirm the test
fails, then restore it. A test that passes against broken code is worse than no test,
because it reports coverage that is not there. Every test in this suite was falsified
when it was added, and two of them caught real defects that way.

The slow tier is `tests/test_backpressure.py`. It is the only place a real queue overflow
is forced against a real socket, and it is where the isolation guarantee either holds or
does not — so CI runs it. `test-fast` skips it for an inner loop, not for a PR.

### The invariant tests

`tests/test_invariants.py` reads the source's AST and asserts that `Stream.send` contains
no `await`, and that joining the fan-out set and reading the frontier are adjacent
statements. Those look like strange tests until you have chased the bug they prevent:
both claims compile fine when broken and produce a reordering or a duplicate only under a
race nobody will reproduce on purpose. [`docs/SPEC.md`](docs/SPEC.md) §3 is the argument.

**If a change makes one of them fail, the change is probably wrong.** If it is right, the
spec section has to change with it, in the same PR.

## Performance

```bash
just bench
```

The publish path is one SQLite transaction at `synchronous=FULL` and the fsync dominates,
so `send_many` is the lever and the interesting number is the distance from a raw
litelink `extend`. The fan-out path is memory, and the interesting number is whether it
is linear in subscribers with a small constant.

Measure before and after in the same session on the same machine — these numbers move
with hardware, and a comparison across two runs on two boxes says nothing. If a change
costs throughput, say so in the commit with the figures.

## Documentation

Three places, and a change usually touches one:

- [`docs/SPEC.md`](docs/SPEC.md) — the design, the protocol and the invariants. Behaviour
  changes belong here, in the section that claimed the old behaviour.
- [`docs/API.md`](docs/API.md) — every public call, on one page.
- [`README.md`](README.md) — the front door. What it is, how to start, what it is not.

A PR that changes what the library does and leaves the spec describing the old behaviour
will be asked to fix the spec, because a document that lies is worse than a missing one.

## Pull requests

Branch from `main`, keep the PR to one thread of work, and make sure `just check` is
green before pushing — CI runs the same gates on Python 3.11 and 3.13 plus a packaging
job that installs the wheel into a clean environment and runs a stream through it.
`CI success` is the required check.

Say what you changed and why in the description. If you found something on the way that
you did not fix, write it down rather than leaving it for the next person to rediscover.
