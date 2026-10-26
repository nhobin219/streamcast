"""Credential-isolated maintainer and replication child factories."""

from __future__ import annotations

import warnings
from pathlib import Path

from streamcast._maintain import ROLES, Maintain, Supervisor
from streamcast._process import environment
from streamcast._replicate import Sidecar
from streamcast._stream import StreamHandle


def _supervisors(
    routes: dict[str, StreamHandle], maintain: bool | Maintain
) -> list[Supervisor]:
    """FIVE maintainers — one per role of litelink's split — for every log
    this server serves, plus five more for each dedicated log.

    A live-only stream has nothing to sweep, so a server of them spawns
    nothing rather than processes with no work to do.

    **Five per group, not five per log.** Streams with different `S3Options`
    form separate groups. A maintainer is a full interpreter
    with litelink, pyarrow, pyiceberg and duckdb loaded — ~150-200 MB RSS
    each — so each role's process covers every log, and a new stream costs a
    row in five loops rather than five more interpreters. The roles are split
    from each other because the work in them is, on CPU or the network; the
    logs need no such isolation. See `_maintain`.

    `Maintain.dedicated` names logs that still get their own five, for one
    large or hot enough that its compaction would hold up everyone else's.

    WAL replication is a separate concern with its own argument — see
    `_sidecars`, which runs litestream for the logs that need it.
    """
    if maintain is False:
        return []

    plan = Maintain() if maintain is True else maintain
    # A retired stream's log takes no more passes: it is finished, and
    # opened only for reading.
    logs = [
        stream.log
        for stream in routes.values()
        if stream.log is not None and stream.retirement is None
    ]

    # **A name that matches nothing is a raise, not a shrug.** Silently
    # ignoring it puts the log back in the shared loop — the one thing the
    # caller named it to avoid — and the symptom is a latency problem they
    # believe they already fixed. `_routes` refuses a name collision rather
    # than resolving one for the same reason.
    served = {log.name for log in logs}
    unknown = sorted(set(plan.dedicated) - served)
    if unknown:
        msg = (
            f"maintain=Maintain(dedicated=...) names {', '.join(map(repr, unknown))}, "
            f"which this server does not serve with a log. Served with a log: "
            f"{', '.join(map(repr, sorted(served))) or 'nothing'}. "
            f"Names are the LOG's, which need not be the route it is served at."
        )
        raise ValueError(msg)

    # **One environment per set of `S3Options`**, so a set is a group of its
    # own: the children open the logs by name, and take the options from
    # their environment (`_process.environment`) or not at all.
    # By root and name, as the targets are: a name alone is not unique
    # across roots, and another stream's options would win for both.
    key = {
        (Path(stream.log.root), stream.log.name): _credentials(stream)
        for stream in routes.values()
        if stream.log is not None
    }
    groups = [
        [(Path(log.root), log.name)] for log in logs if log.name in plan.dedicated
    ]
    shared = [
        (Path(log.root), log.name) for log in logs if log.name not in plan.dedicated
    ]
    # **A migrated stream's retired logs are not maintained.** `retire()`
    # publishes everything, evicts staging and sweeps both tables completely
    # before it marks the log retired, and litelink refuses a writer on one
    # from then on: there is nothing left to do, and nothing could open it
    # to try. One `publish` process is handed them anyway, to retire through
    # litelink any that a migration before streamcast 0.10 only sealed — see
    # `_maintain.finish_retiring`.
    shared_groups: dict[tuple[tuple[str, str | None], ...], list[tuple[Path, str]]] = {}
    for target in shared:
        shared_groups.setdefault(key[target], []).append(target)

    dedicated = len(groups)
    groups.extend(shared_groups.values())
    environments = [key[group[0]] for group in groups]

    # Each stream's retired logs to the publish role of a group with its
    # options: the shared group with them if there is one, else the first
    # with them, else — a retired stream whose options no served log has —
    # the first.
    owners: dict[tuple[tuple[str, str | None], ...], int] = {}
    for index, wanted in enumerate(environments):
        if index >= dedicated or wanted not in owners:
            owners[wanted] = index

    retiring: dict[int, list[tuple[Path, str]]] = {}
    for stream in routes.values():
        if stream.retired:
            index = owners.get(_credentials(stream), 0)
            retiring.setdefault(index, []).extend(stream.retired)

    # **Retention runs where the stream's live log is cleaned up** — the
    # slow, published-table role — with the handle its live log is truncated
    # through. By the stream's name, which its metadata is filed under.
    streams = {
        (Path(stream.log.root), stream.log.name): (
            Path(stream.log.root),
            stream.name or stream.log.name,
        )
        for stream in routes.values()
        if stream.log is not None and stream.retirement is None
    }
    return [
        Supervisor(
            group,
            plan,
            role,
            retiring=retiring.get(index, []) if role == "publish" else (),
            streams=(
                [streams[target] for target in group if target in streams]
                if role == "clean-published"
                else ()
            ),
            env=dict(environments[index]),
        )
        for index, group in enumerate(groups)
        for role in ROLES
    ]


def _sidecars(routes: dict[str, StreamHandle], replicate: bool) -> list[Sidecar]:
    """ONE litestream for every log this server replicates, or none.

    **Opt-in twice:** `wal_replication` on the log, and `replicate=True` on
    the server. Off by default because most deployments replicate nothing, and
    one that does may run its own litestream. A log that asks for replication
    on a server that does not run it is warned about, by name, at every start
    — one line — rather than left believing it is protected.

    `wal_replication` is opt-in on the log, so this is empty for almost every
    deployment and starts nothing. When it is on, the log's whole point is
    surviving the loss of its machine — and a server that came up and
    replicated nothing would leave that belief in place with none of the
    protection, which is why a missing binary raises here rather than at the
    first missed push.

    **One process, not one per log.** litestream takes a `dbs` list, and each
    process was 40-170 MB — so four small streams spent most of a gigabyte
    replicating a producer of ~460 MB. The lock that keeps two litestreams off
    one database stays PER LOG, because it protects the database rather than
    the replicator; `Sidecar` holds one per log and replicates exactly the
    ones it holds.
    """
    shipping = [
        stream.log
        for stream in routes.values()
        if stream.log is not None
        and stream.retirement is None
        and stream.log.config.wal_replication
    ]
    if not replicate:
        if shipping:
            names = ", ".join(sorted(log.name for log in shipping))
            warnings.warn(
                f"log(s) {names} have wal_replication on, and this server was "
                f"started without replicate=True: nothing here replicates them. "
                f"Pass replicate=True, or run litestream for them yourself.",
                stacklevel=3,
            )

        return []

    # One per set of `S3Options`, as the maintainer is: litestream takes its
    # credentials from its environment, and one environment holds one set.
    by_options: dict[tuple[tuple[str, str | None], ...], list[StreamHandle]] = {}
    for stream in routes.values():
        if stream.log is not None and stream.log in shipping:
            by_options.setdefault(_credentials(stream), []).append(stream)

    return [
        Sidecar.new(
            [stream.log for stream in streams if stream.log is not None],
            s3_options=streams[0]._s3,  # noqa: SLF001
        )
        for streams in by_options.values()
    ]


def _credentials(stream: StreamHandle) -> tuple[tuple[str, str | None], ...]:
    """What a child of `stream` needs in its environment, as a grouping key."""
    return tuple(sorted(environment(stream._s3).items()))  # noqa: SLF001
