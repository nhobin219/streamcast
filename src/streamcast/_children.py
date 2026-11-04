"""Immutable child identities, credential-isolated planning and one factory."""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol

from streamcast._maintain import FLUSHED, ROLES, Maintain, Supervisor
from streamcast._process import environment
from streamcast._replicate import Sidecar
from streamcast._spec import Maintenance
from streamcast._stream import StreamHandle

Target = tuple[Path, str]
Credentials = tuple[tuple[str, str | None], ...]


class Generation(Protocol):
    stream: StreamHandle
    maintenance: Maintenance


@dataclass(frozen=True)
class ChildSpec:
    """Only inputs effective for this child participate in its identity.

    In particular, sidecars have no maintenance cadence, and a role does not
    inherit another role's cadence. Credentials are private even in reprs.
    """

    kind: str
    members: tuple[Target, ...]
    credentials: Credentials = field(repr=False)
    role: str | None = None
    every: float | None = None
    flush: float | None = None
    retiring: tuple[Target, ...] = ()
    streams: tuple[Target, ...] = ()


def desired(
    generations: Mapping[str, Generation], defaults: bool | Maintain
) -> set[ChildSpec]:
    """Plan without constructing children, touching files or spawning processes."""
    routes = {name: g.stream for name, g in sorted(generations.items())}
    return _maintenance(
        routes, defaults, {name: g.maintenance for name, g in generations.items()}
    ) | _replication(routes)


def _maintenance(
    routes: Mapping[str, StreamHandle],
    maintain: bool | Maintain,
    policies: Mapping[str, Maintenance],
) -> set[ChildSpec]:
    if maintain is False:
        return set()

    plan = Maintain() if maintain is True else maintain
    active = {
        (Path(stream.log.root), stream.log.name): stream
        for stream in routes.values()
        if stream.log is not None and stream.retirement is None
    }

    overrides = {
        (Path(stream.log.root), stream.log.name): policies[name]
        for name, stream in routes.items()
        if stream.log is not None and stream.retirement is None
    }
    # Dedication belongs to the stream definition, never the physical log name.
    groups: list[tuple[Target, ...]] = [
        (target,) for target in sorted(active) if overrides[target].dedicated
    ]
    shared: dict[Credentials, list[Target]] = {}
    for target, stream in sorted(active.items()):
        if not overrides[target].dedicated:
            shared.setdefault(_credentials(stream), []).append(target)

    dedicated = len(groups)
    groups.extend(tuple(group) for group in shared.values())
    plans = [
        replace(
            plan,
            **{
                name: value
                for name, value in overrides[group[0]].model_dump().items()
                if name != "dedicated" and value is not None
            },
        )
        if index < dedicated
        else plan
        for index, group in enumerate(groups)
    ]
    keys = [_credentials(active[group[0]]) for group in groups]
    owners: dict[Credentials, int] = {}
    for index, key in enumerate(keys):
        if index >= dedicated or key not in owners:
            owners[key] = index

    retiring: dict[int, list[Target]] = {}
    for stream in routes.values():
        if stream.retired:
            retiring.setdefault(owners.get(_credentials(stream), 0), []).extend(
                stream.retired
            )

    return {
        ChildSpec(
            kind="maintainer",
            role=role,
            members=group,
            credentials=keys[index],
            every=plans[index].every(role),
            flush=plans[index].flush_every if role in FLUSHED else None,
            retiring=tuple(sorted(retiring.get(index, ())))
            if role == "publish"
            else (),
            # A migration's log name differs from its stream metadata name.
            streams=tuple(
                (target[0], active[target].name or target[1]) for target in group
            )
            if role == "clean-published"
            else (),
        )
        for index, group in enumerate(groups)
        for role in ROLES
    }


def _replication(routes: Mapping[str, StreamHandle]) -> set[ChildSpec]:
    groups: dict[Credentials, list[Target]] = {}
    for stream in routes.values():
        if (
            stream.log is not None
            and stream.retirement is None
            and stream.log.config.wal_replication
            and stream.replicate
        ):
            groups.setdefault(_credentials(stream), []).append(
                (Path(stream.log.root), stream.log.name)
            )

    return {
        ChildSpec("sidecar", tuple(sorted(members)), key)
        for key, members in groups.items()
    }


def build(spec: ChildSpec, routes: Mapping[str, StreamHandle]) -> Supervisor | Sidecar:
    """Construct from the spec; the caller owns start, readiness and cleanup."""
    if spec.kind == "maintainer":
        assert spec.role is not None
        plan = replace(
            Maintain(flush_every=spec.flush),
            **{f"{spec.role.replace('-', '_')}_every": spec.every},
        )
        return Supervisor(
            spec.members,
            plan,
            spec.role,
            retiring=spec.retiring,
            streams=spec.streams,
            env=dict(spec.credentials),
        )

    logs = {
        (Path(stream.log.root), stream.log.name): stream
        for stream in routes.values()
        if stream.log is not None
    }
    members = [logs[target] for target in spec.members]
    return Sidecar.new(
        [stream.log for stream in members if stream.log is not None],
        s3_options=members[0]._s3,  # noqa: SLF001
    )


def warn_unreplicated(stream: StreamHandle, *, replicate: bool | None = None) -> None:
    if (
        stream.log is not None
        and stream.retirement is None
        and stream.log.config.wal_replication
        and not (stream.replicate if replicate is None else replicate)
    ):
        warnings.warn(
            f"log(s) {stream.log.name} have wal_replication on, and this server was "
            f"started without replicate=True: nothing here replicates them. "
            f"Pass replicate=True, or run litestream for them yourself.",
            stacklevel=3,
        )


def _supervisors(
    routes: dict[str, StreamHandle], maintain: bool | Maintain
) -> list[Supervisor]:
    """Build maintenance alone for callers inspecting the credential grouping."""
    policies = {
        name: stream.definition.spec.maintenance
        if stream.definition is not None
        else Maintenance()
        for name, stream in routes.items()
    }
    return [build(spec, routes) for spec in _maintenance(routes, maintain, policies)]  # ty: ignore[invalid-return-type]


def _sidecars(routes: dict[str, StreamHandle], replicate: bool) -> list[Sidecar]:
    """Build replication alone with the explicit host policy."""
    chosen = {name: stream for name, stream in routes.items() if replicate}
    if not replicate:
        for stream in routes.values():
            warn_unreplicated(stream, replicate=False)

    # The standalone factory's explicit argument overrides handle policy.
    groups: dict[Credentials, list[Target]] = {}
    for stream in chosen.values():
        if (
            stream.log is not None
            and stream.retirement is None
            and stream.log.config.wal_replication
        ):
            groups.setdefault(_credentials(stream), []).append(
                (Path(stream.log.root), stream.log.name)
            )

    return [
        build(ChildSpec("sidecar", tuple(sorted(members)), key), routes)
        for key, members in groups.items()
    ]  # ty: ignore[invalid-return-type]


def _credentials(stream: StreamHandle) -> Credentials:
    return tuple(sorted(environment(stream._s3).items()))  # noqa: SLF001
