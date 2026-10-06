"""One broadcast: the offsets, the subscribers, and the subscribe handshake.

A `Stream` is the whole of streamcast that is not transport. `serve` puts it
behind a WebSocket port and `connect` reads it from the other end, but the
ordering guarantee, the offset assignment and the replay partition are all
decided here — which means they can be tested without a socket, and are.

**The invariant the rest of the library rests on: `_deliver` contains no
`await`, and neither does the pair of statements that attaches a subscriber.**
Both are therefore atomic against the event loop, and that atomicity is not a
performance note — it is the correctness argument:

* A durable row is committed by the stream's writer thread (`_writer`), in
  the order `send` queued it, and handed back in commit order. `_deliver`
  then advances the frontier and offers the frame to every subscriber with
  nothing able to interleave, so two concurrent senders cannot produce a
  subscriber that sees offset 8 before offset 7. A live-only stream, which
  has nothing to commit, does the same step inside `send`.
* In `_attach`, the subscriber joins the fan-out set and the frontier is read
  with nothing able to interleave. Every offset below the frontier has been
  delivered, so it is durable in the log; every offset from it up is either
  already in the new subscriber's queue or not yet delivered, and will be.
  The two sets partition the stream exactly — no gap, no duplicate — and that
  is what makes a resume exactly-once.

If an `await` is ever added inside either, both properties are gone and
nothing will fail loudly. `tests/test_invariants.py` is the guard: it
reads this module's AST and fails on an `await` in either place.

**Why the commit is off the loop.** Every stream a broker serves shares its
event loop, so a durable append on the loop — a SQLite commit and fsync, ~2 ms
— stalled every other stream for its length: measured up to 71 ms of loop lag
with one publisher sending flat out. See `_writer`.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import litelink
from websockets.exceptions import ConnectionClosed
from websockets.frames import CloseCode

from streamcast import (
    _filter,
    _live,
    _log,
    _manifest,
    _metadata,
    _replicate,
    _schema,
    _snapshot,
)
from streamcast._codec import compile_codec
from streamcast._errors import Close, NotReplayable, ProtocolError, StreamRetired
from streamcast._limits import MAX_BACKLOG, MAX_IN_FLIGHT, MAX_INBOUND, MAX_TAIL
from streamcast._protocol import (
    EARLIEST,
    decode,
    decode_publish,
    encode,
    greeting,
    publish_ack,
    publish_error,
    refusal,
)
from streamcast._published import ReadCache
from streamcast._stats import Stats
from streamcast._subscriber import Subscriber
from streamcast._writer import Job, Writer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable, Mapping, Sequence
    from os import PathLike

    import pyarrow as pa
    from litelink import (
        LocalReadHandle,
        LogHandle,
        Row,
        S3Options,
        TierStatistics,
        WriteHandle,
    )

    from streamcast._filter import Predicate, Where
    from streamcast._transport import Peer

MAX_REPLAY: Final = 100_000
"""How far back a subscribe may ask to resume from.

It bounds the server, not the log: a replay is a DuckDB scan in a worker
thread and a subscriber asking for ten million rows would hold one for
minutes. Past this, the answer is to read the log directly — which needs
nothing from streamcast and is what litelink is for.

**It is sized against `MAX_BACKLOG`, not independently.** A replay streams
while live messages queue behind it, so a subscriber that takes longer to
catch up than `MAX_BACKLOG` messages of live traffic is dropped the moment it
arrives, having done all the work. The defaults hold with room: a replay reads
at ~1M rows/s from local Parquet, so 100,000 messages is ~0.1 s, and a feed
would have to run above 80,000 messages/s to put 8,192 messages into the queue
in that time. Raise one and check the other.
"""


class Stream:
    """A broadcast, and the offsets that make it resumable.

    Constructed before there is a loop and serves any number of subscribers on
    whichever loop `serve` runs on. It owns no thread and starts nothing —
    `serve` is what starts the maintainer and, with `replicate=True`, the
    litestream sidecar, and it stops them again.

        stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA)

        async with streamcast.serve(stream, "localhost", 8765):
            async for frame in upstream:
                await stream.send(parse(frame))      # a row, not a blob

    **The schema is yours, and declared in JSON Schema.** The log is an
    ordinary litelink table with whatever shape you gave it, so every column
    prunes, compresses and is queryable from any Iceberg engine. A row goes in
    and the same row comes back out, live or replayed. The table also carries
    `streamcast_ts`, the time the server took each row — stored, and sent
    beside the row as each frame's `ts`; see `_log.STAMP`. `_schema` converts the declaration to Arrow, so a durable stream
    needs no import but this one.

    **Who closes the log depends on who opened it.** `root=`+`schema=` creates
    it here — `new` the first time, `open` every time after, which is the
    try/except every caller otherwise writes — and `aclose` closes what it
    created. Pass `log=` an open handle instead and it stays the caller's, for
    a process that reads or writes it through litelink as well. The two are
    mutually exclusive, because a `schema=` beside a `log=` is a declaration
    that cannot be enforced: `open` reads the shape off disk.

    **`log` is what makes an offset a resume cursor**, and it is optional in
    both directions: a tickerplant's log is optional too — some kdb
    implementations omit it — so a stream without one is not a lesser thing,
    just one nothing can resume from. Without it
    nothing assigns offsets at all — `send` returns None and every frame
    carries `null` — so `?offset=` is refused outright rather than appearing to
    work until the day a subscriber needs it. With it, every row is durable
    *before* any subscriber sees it — so a server that dies
    between the two has published nothing it cannot replay, which is the
    ordering that makes recovery a replay rather than a reconciliation.
    """

    __slots__ = (
        "_codec",
        "_columns",
        "_declared",
        "_end_offset",
        "_floor",
        "_last_send",
        "_last_send_ts",
        "_log",
        "_max_backlog",
        "_max_in_flight",
        "_max_inbound",
        "_max_replay",
        "_name",
        "_owned",
        "_replay_published",
        "_retired",
        "_retirement",
        "_stream_id",
        "_s3",
        "_shape",
        "_stamped",
        "_started",
        "_started_ts",
        "_subscribers",
        "_validate",
        "_writer",
        "_group_commit",
        "_queued",
        "_waiting",
        "_check_stored",
    )

    def __init__(
        self,
        name: str = "",
        *,
        log: WriteHandle | LocalReadHandle | None = None,
        owns_log: bool = False,
        max_replay: int | None = MAX_REPLAY,
        floor: int | None = None,
        retired: Sequence[tuple[Path, str]] = (),
        s3_options: S3Options | None = None,
        schema: Mapping[str, object] | None = None,
        replay_published: bool = False,
        group_commit: bool = True,
        retirement: _metadata.Retirement | None = None,
    ) -> None:
        """Takes an already-open log and builds nothing. See `Stream.new`.

        litelink's rule, which this used to break: it took `root=`+`schema=`
        and called `litelink.new`/`open` from here, so constructing a
        `Stream` created directories and a SQLite database. That moved to
        `new`, which is where litelink puts the same work.

        `owns_log` is the one piece of lifetime this object holds: `aclose`
        closes a log it owns and never one it was merely lent. `new` sets it;
        a caller passing `log=` can set it too, to hand over the lifetime of
        a handle they opened.

        `floor` and `retired` describe a MIGRATED stream, and `new`, `migrate`
        and `restore` read both off its metadata. `floor` is the offset the
        current log starts at: below it the rows are in a retired log, which
        this server does not replay from. `retired` is the `(root, name)` of
        each retired log still on this disk, which `serve`'s maintainer keeps
        maintaining so their retention and eviction carry on.

        `s3_options` is what `serve` uploads the stream's metadata file with, when the
        log publishes to S3. None resolves from the environment, as litelink
        does. It is kept, not used here: this initialiser does no I/O.

        `replay_published` says whether a replay may read the log's published
        table, below what the server holds locally; see `Stream.new`.

        `schema` is for a stream WITHOUT a log: declared in JSON Schema, as
        `Stream.new` takes it, and held to exactly the rule a log would hold
        it to — see `send`. A stream with a log carries its own shape, so
        passing both is refused: two declarations, one of which would be
        silently ignored.
        """
        if log is not None and schema is not None:
            msg = (
                "a stream with a log takes its schema from the log; pass log= "
                "or schema=, not both"
            )
            raise ValueError(msg)

        # A live-only stream's declared columns, if it has any. Converted
        # here, not deferred: it is a pure transformation, and a declaration
        # that cannot be a stream should fail at construction.
        declared_live = None if schema is None else _declaration(schema)
        # The declared column order, read once. It fixes the key order of
        # every frame, and a replayed row must encode to the same bytes as the
        # live one it repeats (I6) — so this cannot be re-derived per message
        # from whatever keys a caller's dict happened to carry.
        #
        # None for a stream with no log: there is no declared schema, so a
        # frame takes the row's own order. Such a stream is a multicaster and
        # nothing replays from it, so there is no second encoding to match.
        self._columns = (
            _log.columns(log)
            if log is not None
            else None
            if declared_live is None
            else tuple(declared_live.names)
        )
        # The declared columns as Arrow — what `where=` and the codec read —
        # and the wire conversions they need, compiled once. A stream of
        # scalars gets `NONE`, and pays for none of it on the hot path.
        self._declared = _log.declared(log.schema) if log is not None else declared_live
        self._codec = compile_codec(self._declared)
        # **The same check a log would make, with no log.** litelink's own
        # `validate_row` — the DDL and helpers `append` uses — so a live-only
        # stream accepts exactly the rows a durable one would, and attaching
        # a log later changes nothing about what is accepted. None when there
        # is nothing to check against: a stream with a log, whose `append`
        # checks, or one with no schema, which is shape-agnostic on purpose.
        self._validate = (
            None
            if declared_live is None
            else partial(litelink.validate_row, declared_live)
        )
        # **A durable row is checked on the loop, before it is queued**, against
        # the log's whole schema — what `append` itself checks. The writer
        # commits queued rows in groups, and one bad row in a group would fail
        # every other publisher's rows in its transaction; refused here, it
        # fails alone. Started lazily, on the first send: constructing a
        # `Stream` still starts nothing.
        self._check_stored = (
            None if log is None else partial(litelink.validate_row, log.schema)
        )
        # Sends that queue behind a commit in flight share the next one, unless
        # the stream opts out — then each send is its own transaction. The
        # greeting says which promise this stream makes.
        self._group_commit = group_commit and log is not None
        # Rows queued for the writer and not yet delivered, and the senders
        # waiting for that to fall below `max_inbound`. See `_room`.
        self._queued = 0
        self._waiting: list[asyncio.Future[None]] = []
        # A retired stream is read-only: its log is opened for reading, and
        # `_submit` refuses before anything could reach a writer.
        self._retirement = retirement
        self._writer = (
            None
            if log is None or retirement is not None
            else Writer(log, self._deliver, self._fail, group=group_commit)  # ty: ignore[invalid-argument-type]
        )
        # The shape a subscriber is told at subscribe, built once. None for a
        # stream with no log: there are no declared columns to publish. Without
        # `streamcast_ts`, which no frame carries — see `_log.declared`.
        self._shape = (
            None if self._declared is None else _schema.from_arrow(self._declared)
        )
        # Whether `send` fills `streamcast_ts`. Decided once, per log: a log
        # created before the column existed, or one a caller opened and passed
        # in, may not have it, and its shape is not this library's to change.
        self._stamped = log is not None and _log.stamped(log)
        self._floor = floor
        # Read off the metadata file at `ensure_metadata`, which `serve` calls
        # before the first subscribe; the greeting names it so a reader can
        # check the file it opens is this stream's.
        self._stream_id: str | None = None
        self._replay_published = replay_published
        self._retired = tuple(retired)
        self._s3 = s3_options
        # Closed by `aclose` only when this object owns it. A log the caller
        # opened stays the caller's — they may be sharing it, and a library
        # that closes a handle it was lent is a library you cannot lend one
        # to.
        self._owned = log if (owns_log and log is not None) else None
        self._name = name
        self._log = log
        # The queue bounds, defaults until `serve` sets the deployment's
        # (`_bound`): they belong to the process serving the stream, not to
        # the stream, so they can change on a restart. See `_limits`.
        self._max_backlog = MAX_BACKLOG
        self._max_inbound = MAX_INBOUND
        self._max_in_flight = MAX_IN_FLIGHT
        self._max_replay = max_replay
        # Read ONCE, here, and maintained by `send` thereafter. litelink's
        # `end_offset()` is a SQLite read and `append` returns the offset it
        # assigned, so asking the log per message would be a round trip for a
        # number the previous call already returned. The one risk of a cached
        # counter — drifting from the log — cannot happen, because nothing
        # else writes this log: litelink allows exactly one writer.
        #
        # None with no log, and there is deliberately no counter to stand in.
        # An in-memory sequence would look exactly like a resume cursor to
        # every subscriber and to every operator reading a greeting, and would
        # be wrong the moment the process restarted. Nothing assigned an
        # offset, so nothing reports one.
        #
        # This is the one read left in this initialiser, and it is a read on
        # an INJECTED collaborator rather than the construction of one — a
        # fake log with an `end_offset()` substitutes cleanly, which is the
        # property litelink's rule exists to protect. Deferring it would put
        # a branch on `send`, which is the hot path, to buy nothing.
        self._end_offset: int | None = log.end_offset() if log is not None else None
        self._subscribers: set[Subscriber] = set()
        # Two clocks, deliberately. The monotonic pair is what ages are
        # measured from, so an NTP step cannot turn a live stream into an
        # apparently stale one; the wall-clock pair is what a human reads and
        # what correlates with the caller's own logs. Reading a clock is not
        # building a collaborator, so this stays out of the factory.
        self._started = time.monotonic()
        self._started_ts = time.time()
        # None until the first send, and it MEANS "not in this process" — the
        # log may hold millions of rows from before the last restart. That is
        # exactly why `uptime_s` is published beside it: a large `end_offset`
        # with no send is ordinary four seconds into a restart and alarming
        # six hours in, and neither number says so alone.
        self._last_send: float | None = None
        self._last_send_ts: float | None = None

    @classmethod
    def new(
        cls,
        name: str = "",
        *,
        root: str | PathLike[str],
        schema: Mapping[str, object],
        sort_by: Sequence[str] | None = None,
        config: object | None = None,
        published: str | None = None,
        s3_options: object | None = None,
        max_replay: int | None = MAX_REPLAY,
        replay_published: bool = False,
        group_commit: bool = True,
    ) -> Stream:
        """A stream and the log underneath it, created if it is not there yet.

            stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA)

        **This is where the I/O lives, and that is the whole point of it
        being a separate call.** litelink says it plainly — *"the initialiser
        takes already built collaborators and does no I/O, so a test can
        substitute any of them"* — and puts its own assembly in
        `litelink.new`. This is the same split under the same name.

        Everything but `name` goes to litelink's `new`, with the stream's
        name fed through, so a durable stream needs no import but this one.
        `name` is both the stream's route and the log's name: one fact, one
        home, and a server cannot serve `/trades` off a log called something
        else.

        The returned `Stream` OWNS its log and closes it in `aclose`. Pass
        `log=` to the initialiser instead when the handle is yours to keep.

        `replay_published` is NOT part of `config`: litelink persists a
        `LogConfig` in the log's `meta` table, so a field there would be
        durable policy shared by every process that opens the log, and one
        caller's `set_config` would change another's read tier. Which tiers a
        handle reads is a property of that handle, so it is passed and not
        stored — reopen with it to get it again.

        **`replay_published=True` with `max_replay=None` makes the server a
        complete gateway to the log.** Together they mean no subscribe is ever
        refused for reaching too far back: the server reads whatever the
        published table holds and streams it as ordinary JSON frames. A client in any
        language replays the entire history over a plain WebSocket — no
        litelink, no Iceberg reader, no object-storage credentials, no
        dependency on this repo. `catch_up` exists because the default is the
        opposite; this is the setting that makes it unnecessary.

        It is not the default, and the reason is `max_backlog`. A replay is
        served before the live queue, which fills behind it — so a subscriber
        reading ten million rows out of S3 accumulates live messages for as
        long as that takes, and is dropped the moment it catches up if it
        passed `max_backlog` on the way. **Size the two together**: a server
        meant to serve whole-history replays wants a backlog matched to the
        longest replay it will serve, or a stream quiet enough that the
        arithmetic does not bite. The other cost is thread-shaped — each
        replay holds a worker from the `to_thread` pool for its whole scan,
        and that pool is `min(32, cpu + 4)`.
        """
        retired = _open_retired(root, name, schema=schema)
        if retired is not None:
            log, metadata = retired
            return cls._read_only(
                name,
                root,
                log,
                metadata,
                s3_options=s3_options,
                max_replay=max_replay,
                replay_published=replay_published,
            )

        log, metadata = _open_or_create(
            root,
            name,
            schema=schema,
            sort_by=sort_by,
            config=config,
            published=published,
            s3_options=s3_options,
        )

        return cls(
            name,
            log=log,
            owns_log=True,
            max_replay=max_replay,
            floor=_floor(metadata),
            retired=_retired(root, metadata),
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            replay_published=replay_published,
            group_commit=group_commit,
        )

    @classmethod
    def migrate(
        cls,
        name: str = "",
        *,
        root: str | PathLike[str],
        schema: Mapping[str, object],
        sort_by: Sequence[str] | None = None,
        config: object | None = None,
        s3_options: object | None = None,
        max_replay: int | None = MAX_REPLAY,
        replay_published: bool = False,
        group_commit: bool = True,
    ) -> Stream:
        """Move a stream onto a new log with a new schema, and return it.

            stream = streamcast.Stream.migrate("trades", root="data", schema=V2)
            async with streamcast.serve(stream, "localhost", 8765):
                ...

        **With the server stopped.** Nothing is migrated live: publishers are
        sending the old shape until they are changed too, so a migration is a
        deploy — stop the server, migrate, serve the result.

        Three steps, in this order:

        1. **The current log is retired.** Every buffered row is sealed, the
           whole log is published, and litelink refuses any writer from then
           on.
        2. **The next log is created** — `trades-v2`, then `-v3` — with the new
           schema, starting at EXACTLY the offset the old one ended at. The
           offsets stay one dense sequence across the seam: no fence, because
           with the server stopped nothing can send in between.
        3. **The metadata records both**, the new one as current, locally and
           beside the published tables on S3. See `_metadata`.

        **Idempotent**, so it can sit in a server's startup: a stream whose
        current log already has this schema AND every system column there is
        today (`_log.SYSTEM`) is opened, not migrated again. So migrating with
        an unchanged schema is the upgrade: a log from before a system column
        existed — `streamcast_ts`, or any added later — moves onto one that
        has it.

        **What may change: columns added, columns removed, and nullability.**
        A column's TYPE is fixed for the life of the stream, including after
        it is removed — re-adding a name takes the type it had. A stream's
        logs are read together with `UNION ALL BY NAME`, where a changed type
        coerces silently rather than failing; see `_metadata.check_types`.

        **There is no rename.** A column under a new name is one column
        removed and another added, and nothing is backfilled or merged: a
        read across the seam returns both, each null in the logs that did not
        have it. Treating them as one — `coalesce(px, price)` — is the
        application's decision, made on the table it reads back.

        `config` and `sort_by` default to the current log's, and the published
        location is the current log's — the metadata lives beside it — unless
        that was litelink's local default, which lives inside the old log's
        directory; then the new log gets its own.

        **This server replays only the current log.** A subscriber resuming
        from below the seam is refused `evicted`, naming where the current log
        starts; `catch_up=True` reads below it through `Stream.snapshot`,
        which reads every log. A consumer that was caught up when the server
        stopped resumes exactly at the seam and loses nothing.
        """
        opened = _open_retired(root, name, schema=schema)
        if opened is not None:
            # Retired, so served as it is: never migrated onto a new log, which
            # is what reviving does (`restore(..., revive=True)`).
            log, found = opened
            return cls._read_only(
                name,
                root,
                log,
                found,
                s3_options=s3_options,
                max_replay=max_replay,
                replay_published=replay_published,
            )

        declared = _declaration(schema)
        metadata = _metadata.load(root, name)
        current = name if metadata is None else metadata.current.name
        retired = False
        try:
            old: LogHandle | None = litelink.open(root, current)
        except litelink.RetiredError:
            # **A migration that died after retiring this log** and before
            # committing the metadata. The retire is done and cannot be undone
            # — litelink refuses a writer on the log from here on — so this
            # run carries on from creating, or adopting, its successor.
            old = litelink.open(root, current, read_only=True)
            retired = True
        except FileNotFoundError:
            msg = (
                f"there is no stream {name!r} at {root} to migrate. "
                f"Stream.new creates one."
            )
            raise FileNotFoundError(msg) from None

        try:
            if metadata is None:
                # Never served under this version, so no metadata file yet:
                # one log at the stream's name, described now.
                metadata = _metadata.single(name, old)

            _metadata.check_types(metadata.logs, _schema.from_arrow(declared))

            if (
                not retired
                and isinstance(old, litelink.WriteHandle)
                and list(_log.declared(old.schema)) == list(declared)
                and _log.is_current(old)
                and (sort_by is None or tuple(sort_by) == old.sort_by)
            ):
                # Already this shape: opened, not migrated again. Nothing is
                # written here — `serve` saves and syncs the metadata of every
                # stream it starts, which also finishes a migration that died
                # between saving it and uploading it.

                opened = old
                old = None
                return cls(
                    name,
                    log=opened,
                    owns_log=True,
                    max_replay=max_replay,
                    floor=_floor(metadata),
                    retired=_retired(root, metadata),
                    s3_options=s3_options,  # ty: ignore[invalid-argument-type]
                    replay_published=replay_published,
                    group_commit=group_commit,
                )

            retired_schema = old.schema
            new_log, sealed = _seal_and_succeed(
                old,
                root,
                metadata.next_name(),
                declared=declared,
                sort_by=sort_by,
                config=config,
                s3_options=s3_options,
                retired=retired,
            )
        finally:
            if old is not None:
                old.close()

        start = new_log.end_offset()
        metadata = metadata.advance(
            start,
            _metadata.describe(
                new_log.name,
                start,
                None,
                new_log.schema,
                published=new_log.published,
                sort_by=new_log.sort_by,
            ),
            span=_metadata.span(sealed),
        )
        # The retired log's statistics join the manifest, which is written
        # BEFORE `metadata.json`: the metadata is the commit, and it must
        # never point at a manifest that does not exist yet (#27).
        retired = metadata.sealed_logs[-1]
        manifest = _manifest.extend(
            _manifest.load(root, name),
            _manifest.entry(
                retired.name,
                retired.start_offset,
                retired.end_offset,
                retired_schema,
                sealed,
            ),
        )
        metadata = dataclasses.replace(metadata, manifest=_manifest.name(name))
        try:
            _manifest.save(root, name, manifest)
            if _metadata.remote(new_log.published):
                _manifest.publish(new_log.published, name, manifest, s3_options)  # ty: ignore[invalid-argument-type]

            _metadata.save(root, metadata)
            if _metadata.remote(new_log.published):
                _metadata.publish(metadata, new_log.published, s3_options)  # ty: ignore[invalid-argument-type]

        except BaseException:
            new_log.close()
            raise

        return cls(
            name,
            log=new_log,
            owns_log=True,
            max_replay=max_replay,
            floor=_floor(metadata),
            retired=_retired(root, metadata),
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            replay_published=replay_published,
            group_commit=group_commit,
        )

    @classmethod
    def restore(
        cls,
        name: str = "",
        *,
        root: str | PathLike[str],
        published: str,
        s3_options: object | None = None,
        binary: str | None = None,
        replay_published: bool = False,
        group_commit: bool = True,
        max_replay: int | None = MAX_REPLAY,
        schema: Mapping[str, object] | None = None,
        sort_by: Sequence[str] | None = None,
        config: object | None = None,
        replica_reserve: int | None = None,
        published_reserve: int | None = None,
        revive: bool = False,
    ) -> Stream:
        """Stand a stream up on a box that never held its log.

            stream = streamcast.Stream.restore(
                "trades", root="data", published="s3://market-data/prod"
            )

        Producer-side failover, and the counterpart to `connect(cursor=)` on
        the consumer side. `Stream.new` needs the log to be here already;
        this rebuilds it from its replicated WAL when there is one, and
        otherwise from its published table alone (litelink 0.10), then hands
        back a stream ready to `serve` and `send` to.

        **Offsets are fenced, not reissued, and that is what makes the move
        invisible to consumers.** litelink skips `replica_reserve` (2**20)
        past what a replica recorded, or `published_reserve` (2**40) past what
        the published table says the log issued — rows written after the last
        publish are gone with the machine — so the restored stream resumes
        above anything the dead machine may have served. A consumer reconnects with the cursor it already had, sees a
        gap, and carries on — no offset it holds is ever reused for different
        data, which is the one thing a resume cannot survive. `recv` allows a
        forward jump for exactly this reason.

        **Rows inside the replication lag are lost.** Anything appended after
        the last WAL frame shipped was served to callers and never left the
        box. A PLANNED cutover has none: stop the writer, let the sidecar ship
        its last frames, then restore. Only unplanned failover loses rows, and
        it loses the ones the old box never managed to replicate.

        The staging table comes back EMPTY — its Parquet was on the dead
        machine — so a replay from local tiers sees nothing below the buffer.
        Pass `replay_published=True` to serve history from the published
        table. Nothing copies published files back down.

        **The log's shape comes from the stream's metadata**: its exact
        schema — binary encodings and system columns included, which an
        Iceberg schema does not keep — and its `sort_by`. litelink checks them
        against the replica or the table, and needs them outright for a table
        no litelink 0.10 publish stamped. `schema=` (JSON Schema, as `new`
        takes) and `sort_by=` are for a stream whose metadata predates
        recording them, or to override; `config=` replaces the restored
        log's policy, which is otherwise the replica's, or litelink's default.

        **A retired stream is refused** unless `revive=True`, which continues
        it on its next log at exactly the retired end — no fence, since
        retiring fixed the end — with the retired log's columns and sort and
        `config`. See `Stream.retire`.

        ⚠️ **Two writers on one log corrupts it.** The fence stops offsets
        being reused; nothing stops the machine you are failing over FROM if
        it is still alive. litelink cannot detect a live writer on another
        host — measured: a restore against a live primary succeeds, both
        handles append, and both publish to the same table. Stop the old
        producer first. See `docs/SPEC.md`.
        """
        # **The metadata first**, because it says which log is current. Without
        # it this would rebuild a migrated stream's FIRST log and serve that as
        # though nothing had happened since.
        metadata = (
            _metadata.fetch(published, name, s3_options)  # ty: ignore[invalid-argument-type]
            if _metadata.remote(published)
            else None
        )
        if metadata is None:
            # A local published location has no copy beside its tables; a box
            # that holds the stream has the metadata itself. Read for one
            # thing only — whether it is retired — so a revive works there.
            local = _metadata.load(root, name)
            if local is not None and local.retirement is not None:
                metadata = local

        if metadata is not None and metadata.retirement is not None:
            if not revive:
                retirement = metadata.retirement
                raise StreamRetired(name, retirement.at, retirement.end_offset)

            return cls._revive(
                name,
                root,
                metadata,
                published=published,
                config=config,
                s3_options=s3_options,
                max_replay=max_replay,
                replay_published=replay_published,
                group_commit=group_commit,
            )

        # The shape, exactly as the stream recorded it, unless the caller
        # says otherwise; a part not recorded is left to litelink to read.
        current = None if metadata is None else metadata.current
        shape = (
            _log.with_system(_declaration(schema))
            if schema is not None
            else None
            if current is None
            else _metadata.shape(current)
        )
        if sort_by is None and current is not None:
            sort_by = current.sort_by

        # litelink's own reserves unless the caller set them: one owner.
        reserves: dict[str, Any] = {
            key: value
            for key, value in (
                ("replica_reserve", replica_reserve),
                ("published_reserve", published_reserve),
            )
            if value is not None
        }
        log = litelink.restore(
            root,
            name if current is None else current.name,
            published=published,
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            binary=binary,
            schema=shape,
            sort_by=sort_by,
            config=config,  # ty: ignore[invalid-argument-type]
            **reserves,
        )
        if metadata is not None:
            _metadata.save(root, metadata)

        return cls(
            name,
            log=log,
            owns_log=True,
            max_replay=max_replay,
            floor=_floor(metadata),
            retired=_retired(root, metadata),
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            replay_published=replay_published,
            group_commit=group_commit,
        )

    @classmethod
    def retire(
        cls,
        name: str = "",
        *,
        root: str | PathLike[str],
        s3_options: object | None = None,
    ) -> _metadata.Retirement:
        """Finish a stream for good: every row published, nothing more taken.

            streamcast.Stream.retire("trades", root="data")

        Run with the server stopped, as `migrate` is. In order:

        1. **The published table is made complete.** litelink's `retire`
           seals every buffered row and publishes the whole log — the
           trailing run a plain `publish` holds back included — so the table
           every other machine reads ends exactly where the log does. A stream
           that merely stops being written leaves that tail on this disk.
        2. **The log refuses writers**, from any handle, for good.
        3. **The metadata records it** — when, where the log ended, how it was
           sorted and the `streamcast_ts` span — locally and beside the
           tables, with the log's statistics added to the manifest. That is
           everything reviving needs, on a box that never held the log.

        From then on the stream is served READ-ONLY: `new` and `migrate` open
        it for reading — subscribers replay and catch up, a live view simply
        sees nothing new — every send raises `StreamRetired`, and `serve`
        refuses a publisher with 4410. Its maintainers do not run.

        **Undone by `restore(..., revive=True)`**, here or on another box: the
        stream continues on a new log starting where this one ended. Retiring
        published everything first, so reviving loses nothing.

        Safe to run again: an already retired stream returns its record, and
        a retire that died partway finishes.
        """
        metadata = _metadata.load(root, name)
        if metadata is not None and metadata.retirement is not None:
            return metadata.retirement

        current = name if metadata is None else metadata.current.name
        finished = False
        try:
            log: LogHandle = litelink.open(root, current)
        except litelink.RetiredError:
            # litelink retired it — a retire that died before the metadata
            # was written. Only the record is left to make.
            log = litelink.open(root, current, read_only=True)
            finished = True
        except FileNotFoundError:
            msg = f"there is no stream {name!r} at {root} to retire"
            raise FileNotFoundError(msg) from None

        try:
            if metadata is None:
                metadata = _metadata.single(name, log)

            if not finished and isinstance(log, litelink.WriteHandle):
                if log.config.wal_replication:
                    with _replicate.retiring(log):
                        log.retire()
                else:
                    log.retire()

            # The whole log, now all of it published.
            statistics = log.column_statistics()
            end = log.end_offset()
            schema = log.schema
            sort_by = tuple(log.sort_by) or None
            published = log.published
        finally:
            log.close()

        start_ts, end_ts = _metadata.span(statistics)
        live = metadata.live_log
        retirement = _metadata.Retirement(
            at=time.time_ns() // 1000,
            end_offset=end,
            sort_by=sort_by,
            start_ts=live.start_ts if live.start_ts is not None else start_ts,
            end_ts=end_ts,
        )
        # Its statistics join the manifest now, while they can be read: a
        # revived stream seals this log without it, maybe on another box.
        manifest = _manifest.extend(
            _manifest.load(root, name),
            _manifest.entry(live.name, live.start_offset, end, schema, statistics),
        )
        metadata = dataclasses.replace(
            metadata, manifest=_manifest.name(name), retirement=retirement
        )
        # The manifest first, then the metadata that points at it (#27).
        _manifest.save(root, name, manifest)
        if _metadata.remote(published):
            _manifest.publish(published, name, manifest, s3_options)  # ty: ignore[invalid-argument-type]

        _metadata.save(root, metadata)
        if _metadata.remote(published):
            _metadata.publish(metadata, published, s3_options)  # ty: ignore[invalid-argument-type]

        return retirement

    @classmethod
    def _read_only(
        cls,
        name: str,
        root: str | PathLike[str],
        log: LogHandle,
        metadata: _metadata.Metadata,
        *,
        s3_options: object | None,
        max_replay: int | None,
        replay_published: bool,
    ) -> Stream:
        """A retired stream, opened to be read: it replays, and refuses sends.

        Replays from the published table whatever `replay_published` says:
        retiring evicted staging and emptied the buffer, so the table is the
        log's only copy, and a replay of the local tiers would find nothing.
        `replay_published` is taken, and ignored, so the call that opens a
        stream reads the same retired or not.
        """
        del replay_published
        return cls(
            name,
            log=log,  # ty: ignore[invalid-argument-type]
            owns_log=True,
            max_replay=max_replay,
            floor=_floor(metadata),
            retired=_retired(root, metadata),
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            replay_published=True,
            retirement=metadata.retirement,
        )

    @classmethod
    def _revive(
        cls,
        name: str,
        root: str | PathLike[str],
        metadata: _metadata.Metadata,
        *,
        published: str,
        config: object | None,
        s3_options: object | None,
        max_replay: int | None,
        replay_published: bool,
        group_commit: bool,
    ) -> Stream:
        """A retired stream continued on a new log, where the retired one ended.

        The retired log is never written again — litelink refuses — so the
        stream rotates, as `migrate` does: the next name, the same columns
        and sort, starting at exactly the retired end, so the offsets stay
        one dense sequence. Everything it needs is in the metadata, so the
        retired log need not be on this box.
        """
        retirement = metadata.retirement
        assert retirement is not None  # the caller checked
        current = metadata.current
        location = current.published or published
        successor = metadata.next_name()
        declared = _declaration(current.schema)
        if (Path(root) / successor).exists():
            # **A revive that died after creating the log** and before saving
            # the metadata: carried on from, if it is what that run made.
            log = litelink.open(root, successor)
            if (
                log.end_offset() != retirement.end_offset
                or _log.lowest(log) is not None
            ):
                log.close()
                msg = (
                    f"{Path(root) / successor} exists and is not an empty log at "
                    f"offset {retirement.end_offset}, where {current.name!r} was "
                    f"retired; nothing was revived"
                )
                raise FileExistsError(msg)

        else:
            log = litelink.new(
                root,
                successor,
                schema=_log.with_system(declared),
                sort_by=retirement.sort_by,
                config=config,  # ty: ignore[invalid-argument-type]
                published=None if not _metadata.remote(location) else location,
                s3_options=s3_options,  # ty: ignore[invalid-argument-type]
                start_offset=retirement.end_offset,
            )

        revived = metadata.revive(
            _metadata.describe(
                log.name,
                retirement.end_offset,
                None,
                log.schema,
                published=log.published,
                sort_by=log.sort_by,
            )
        )
        try:
            _metadata.save(root, revived)
            if _metadata.remote(log.published):
                _metadata.publish(revived, log.published, s3_options)  # ty: ignore[invalid-argument-type]

        except BaseException:
            log.close()
            raise

        return cls(
            name,
            log=log,
            owns_log=True,
            max_replay=max_replay,
            floor=_floor(revived),
            retired=_retired(root, revived),
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            replay_published=replay_published,
            group_commit=group_commit,
        )

    def __repr__(self) -> str:
        durable = "durable" if self._log is not None else "live-only"
        return (
            f"<streamcast.Stream {self._name!r} {durable} "
            f"end_offset={self._end_offset} subscribers={len(self._subscribers)}>"
        )

    # -- identity ----------------------------------------------------------

    # -- reading a stream's history -------------------------------------------

    @staticmethod
    async def snapshot(
        metadata_uri: str,
        *,
        as_of_offset: int | None = None,
        as_of_ts: int | None = None,
        broker: str | None = None,
        s3_options: S3Options | None = None,
        max_tail: int = MAX_TAIL,
        memory_cache: bool = True,
        disk_cache: bool = False,
        cache_key: str | PathLike[str] | None = None,
        disk_cache_volume_limit: float = 0.8,
    ) -> _snapshot.Snapshot:
        """A stream's history as of one point, read from its published tables.

            snapshot = await Stream.snapshot(uri)                          # all published
            snapshot = await Stream.snapshot(uri, as_of_offset=123)        # that point
            snapshot = await Stream.snapshot(uri, as_of_ts=t)              # by streamcast_ts
            snapshot = await Stream.snapshot(uri, as_of_offset=LATEST, broker=ws)

        `metadata_uri` is the stream's metadata file: `Stream.metadata_uri` on
        the server, or the greeting's on a subscriber. A `Snapshot` reads, it
        does not write: `scan`, `sql` and `rows`, then `close` (or
        `async with`). See `_snapshot` for what each point means, when the
        broker is consulted, and what is refused rather than answered short.

        **Caching is litelink's, and the caller's choice.** `memory_cache`
        (on) keeps what was read in DuckDB's external file cache for the
        process; `disk_cache` (off) keeps `s3://` reads on disk with
        `cache_httpfs`, across restarts, under `cache_key` — a directory
        relative to litelink's cache root (a stream id, say), absolute as
        given, or None for its shared `default`. `disk_cache_volume_limit` is
        how full that disk may get, everything on it counted. Only the
        caller knows what deserves a cache of its own, so nothing is keyed
        for it. Readers with different settings read through different
        databases; see `litelink.duckdb_connection`. The same keywords are on
        `scan`, `sql` and `live`. Not on `connect`: a catch-up reads its gap
        once, which no cache helps.
        """
        return await _snapshot.snapshot(
            metadata_uri,
            as_of_offset=as_of_offset,
            as_of_ts=as_of_ts,
            broker=broker,
            s3_options=s3_options,
            max_tail=max_tail,
            cache=ReadCache.of(
                memory_cache=memory_cache,
                disk_cache=disk_cache,
                cache_key=cache_key,
                disk_cache_volume_limit=disk_cache_volume_limit,
            ),
        )

    @staticmethod
    async def scan(
        metadata_uri: str,
        *,
        as_of_offset: int | None = None,
        as_of_ts: int | None = None,
        broker: str | None = None,
        s3_options: S3Options | None = None,
        max_tail: int = MAX_TAIL,
        memory_cache: bool = True,
        disk_cache: bool = False,
        cache_key: str | PathLike[str] | None = None,
        disk_cache_volume_limit: float = 0.8,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> pa.Table:
        """One `Snapshot.scan` on a snapshot opened for it, read whole, then
        closed: a `pa.Table`, since a reader would outlive its snapshot."""
        async with await Stream.snapshot(
            metadata_uri,
            as_of_offset=as_of_offset,
            as_of_ts=as_of_ts,
            broker=broker,
            s3_options=s3_options,
            max_tail=max_tail,
            memory_cache=memory_cache,
            disk_cache=disk_cache,
            cache_key=cache_key,
            disk_cache_volume_limit=disk_cache_volume_limit,
        ) as snap:
            return await snap.scan(
                columns=columns,
                where=where,
                filters=filters,
                start_offset=start_offset,
                end_offset=end_offset,
            ).read_all()

    @staticmethod
    async def sql(
        metadata_uri: str,
        query: str,
        *,
        as_of_offset: int | None = None,
        as_of_ts: int | None = None,
        broker: str | None = None,
        s3_options: S3Options | None = None,
        max_tail: int = MAX_TAIL,
        memory_cache: bool = True,
        disk_cache: bool = False,
        cache_key: str | PathLike[str] | None = None,
        disk_cache_volume_limit: float = 0.8,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> pa.Table:
        """One `Snapshot.sql` — over the table `log` — read whole, then closed:
        a `pa.Table`, since a reader would outlive its snapshot.

        `filters` and the offsets narrow `log` and prune whole logs; the
        query's own `WHERE` does not prune. See `Snapshot.sql`.
        """
        async with await Stream.snapshot(
            metadata_uri,
            as_of_offset=as_of_offset,
            as_of_ts=as_of_ts,
            broker=broker,
            s3_options=s3_options,
            max_tail=max_tail,
            memory_cache=memory_cache,
            disk_cache=disk_cache,
            cache_key=cache_key,
            disk_cache_volume_limit=disk_cache_volume_limit,
        ) as snap:
            return await snap.sql(
                query,
                filters=filters,
                start_offset=start_offset,
                end_offset=end_offset,
            ).read_all()

    @staticmethod
    async def live(
        broker: str,
        *,
        s3_options: S3Options | None = None,
        rebase_every: float = _live.REBASE_EVERY,
        where: dict[str, object] | None = None,
        start_offset: int | None = None,
        max_tail: int = MAX_TAIL,
        memory_cache: bool = True,
        disk_cache: bool = False,
        cache_key: str | PathLike[str] | None = None,
        disk_cache_volume_limit: float = 0.8,
    ) -> _live.Live:
        """A stream's history kept current in memory: `scan` and `sql` as of now.

            async with await Stream.live("ws://broker:8765/trades") as live:
                await live.sql("SELECT side, sum(amount) FROM log GROUP BY side")
                await live.wait_for(offset)     # until that row is visible

        The published tables as a base, and the broker's rows appended as they
        arrive. The broker is the only address: its greeting names where the
        history is published, read again at every reconnect. Memory holds only what is not yet published: every
        `rebase_every` seconds the base is re-pinned and the rows it covers
        are dropped. A dropped connection reconnects with catch-up; a failure
        it cannot fix is raised by the next query. See `_live`.

        `where=` narrows the view as `connect(where=)` narrows a subscription
        — equality or membership over non-null scalars — on the server and on
        the published tables alike, so every query sees only matching rows.
        `start_offset=` is the lowest offset any query sees; `LATEST` is the
        broker's frontier at open, a view of what happens from now.
        """
        return await _live.live(
            broker,
            s3_options=s3_options,
            rebase_every=rebase_every,
            where=where,
            start_offset=start_offset,
            max_tail=max_tail,
            cache=ReadCache.of(
                memory_cache=memory_cache,
                disk_cache=disk_cache,
                cache_key=cache_key,
                disk_cache_volume_limit=disk_cache_volume_limit,
            ),
        )

    @property
    def metadata_uri(self) -> str | None:
        """Where a reader finds this stream's metadata, or None with no log.

        The copy beside its published tables when they are on `s3://`, else
        the local file as an absolute `file://` URI — readable on this machine
        only, which the `stream_id` check catches anywhere else.
        """
        if self._log is None:
            return None

        return _metadata.uri(self._name, self._log)

    @property
    def name(self) -> str:
        """What this stream is served at. `""` is served at `/`."""
        return self._name

    @property
    def log(self) -> WriteHandle | None:
        """The litelink handle, or None.

        Closed by `aclose` when this `Stream` created it from `root=`+`schema=`,
        and never when it was passed in — that one is the caller's.

        A retired stream's is opened read-only (`litelink.LocalReadHandle`):
        reads work, writes are refused by litelink.
        """
        return self._log  # ty: ignore[invalid-return-type]

    @property
    def schema(self) -> dict[str, object] | None:
        """The stream's shape as JSON Schema, or None without a log.

        What the greeting publishes, so a subscriber in another language can
        read the columns without this repo.
        """
        return None if self._shape is None else dict(self._shape)

    @property
    def retirement(self) -> _metadata.Retirement | None:
        """When the stream was retired and where it ended, or None if it is not.

        Not `retired`, which lists the logs a migration sealed.

        A retired stream is served read-only: it replays, snapshots and
        catches up, and refuses every send with `StreamRetired`. See
        `Stream.retire`.
        """
        return self._retirement

    @property
    def durable(self) -> bool:
        """Whether a log is attached, and so whether offsets survive a restart."""
        return self._log is not None

    @property
    def end_offset(self) -> int | None:
        """The offset the next row will be assigned, or None without a log.

        The same quantity as litelink's `end_offset()`, and deliberately the
        same name — but an attribute here and a call there, because litelink
        reads it from SQLite and this is the counter `send` already maintains.

        None means nothing is assigning offsets, which is a different fact
        from "no rows yet" and should not be confused with 0 or 1.
        """
        return self._end_offset

    def ensure_metadata(self) -> None:
        """Write this stream's metadata file if it has none, and sync its S3 copy.

        What `serve` calls for every stream before it listens; a failure is a
        failure to start. See `_metadata.ensure`. A stream with no log has no
        metadata and this does nothing.

        Also where a `Stream(log=…)` learns it is part of a migrated stream:
        its initialiser does no I/O, so the seam and the retired logs are read
        off the metadata here, before the first subscriber can ask below it.
        """
        log = self._log
        if log is None:
            return

        metadata = _metadata.ensure(self._name or log.name, log, self._s3)
        self._stream_id = metadata.stream_id
        self._floor = _floor(metadata)
        self._retired = _retired(log.root, metadata)

    @property
    def retired(self) -> tuple[tuple[Path, str], ...]:
        """The `(root, name)` of each retired log still on this disk.

        The logs a MIGRATION sealed, not whether this stream is retired —
        that is `retirement`. Empty for a stream that has never migrated.
        `serve` maintains these beside the current log, so their local
        retention keeps running; it never writes to them.
        """
        return self._retired

    @property
    def subscribers(self) -> int:
        """How many are attached right now."""
        return len(self._subscribers)

    # -- publish -----------------------------------------------------------

    async def send(self, row: Row) -> int | None:
        """Make one row durable, then fan it out. Returns its offset.

        **None without a log**, because nothing assigned one. A live-only
        stream fans out and forgets; handing back a per-process counter would
        give the caller a number that behaves like a resume cursor until the
        day the server restarts.

        `row` is a mapping over the log's declared columns — litelink's `Row`,
        the same thing `litelink.append` takes. litelink validates it against
        the schema, so a wrong type or an unknown column raises here with a
        message naming the column, and nothing is broadcast. A live-only
        stream declared with `schema=` is held to the same rule, through
        litelink's `validate_row`.

        **A live-only stream with no schema checks nothing**, on purpose. JSON
        has no NaN or infinity, so a non-finite float sent on one reaches
        subscribers as `null` — declare a schema to have it refused.

        **Durable first.** With a log attached this returns only once the row
        is committed — one SQLite transaction at `synchronous=FULL`, which
        litelink measures at a ~400 us median — and a failure there raises
        with nothing broadcast. That ordering is the reason a crashed server
        is recoverable: a message a subscriber has seen is always a message
        the log holds, never the other way round.

        **It never awaits a consumer.** With a log it awaits its own commit,
        which runs on the stream's writer thread so that no other stream on
        the broker waits on this one's disk (`_writer`); the fan-out is a
        queue insert per subscriber, done on the loop once the commit is back.
        Without a log there is nothing to commit, and it does not await at all.
        With one it may also wait for room: past `serve`'s `max_inbound`
        rows queued for commit, a send waits for the disk — never for a
        consumer.

        The frame is the row as JSON text, encoded once and shared by every
        subscriber (I6) — measured at 0.285 us for a six-column row. The key
        order comes from the log's schema rather than from this dict, which is
        what makes a replay of this row byte-identical to what goes out now.

        Throughput on the durable path is one fsync per call: each send is its
        own transaction while nothing else is committing. `send_many` is the
        lever: it commits a whole group in one. By default (`group_commit`)
        sends from several publishers that queue behind a commit in flight
        share the next transaction; a stream created with `group_commit=False`
        commits each on its own, and its greeting says which.

        **On a stream with no log, a publish loop that never awaits starves
        every subscriber.** There nothing yields, so a
        `for … : await stream.send(…)` over a list in memory runs to
        completion before any pump gets the loop back, and every subscriber
        sees the whole run arrive at once — which for a run longer than
        `max_backlog` means every one of them is dropped for falling behind.
        A real publisher awaits its upstream between messages and never meets
        this. A backfill from memory should `await asyncio.sleep(0)` in its
        loop, or hand the group to `send_many` and let the subscribers take
        it at their own pace.
        """
        await self._room(1)
        [offset] = await self._submit([row])
        return offset

    async def send_many(self, rows: Iterable[Row]) -> list[int | None]:
        """Make a group of rows durable in ONE transaction, then fan each out.

        The write-throughput lever, and it is a call-site choice rather than a
        setting: one fsync for the group instead of one per message. litelink
        measures the same difference on `extend`.

        Each row still gets its own offset and its own frame, so a subscriber
        cannot tell a group from the same rows sent one at a time. That is
        deliberate — batching is the server's durability decision, and making
        it visible on the wire would make every subscriber's parser depend on
        how the publisher happened to poll.
        """
        batch = list(rows)
        if not batch:
            return []

        await self._room(len(batch))
        return list(await self._submit(batch))

    async def _room(self, rows: int) -> None:
        """Wait until `rows` more fit under `max_inbound`. Call `_submit` next,
        with no await between: what was checked here is what is queued there.

        A batch larger than the bound on its own is let in when nothing else
        is queued, so it waits rather than deadlocks. A stream with no log
        queues nothing and never waits.
        """
        while (
            self._writer is not None
            and self._queued
            and self._queued + rows > self._max_inbound
        ):
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiting.append(waiter)
            await waiter

    def _bound(self, *, max_backlog: int, max_inbound: int, max_in_flight: int) -> None:
        """Set the queue bounds `serve` and `asgi` were given for this stream.

        A new `max_backlog` applies to subscribers that join after it, and a
        new `max_in_flight` to publisher connections that open after it. No await.
        """
        self._max_backlog = max_backlog
        self._max_in_flight = max_in_flight
        self._max_inbound = max_inbound

    def _drained(self, rows: int) -> None:
        """`rows` left the queue — committed or failed. Wake every waiter to
        re-check; one that still does not fit waits again. No await."""
        self._queued -= rows
        waiting, self._waiting = self._waiting, []
        for waiter in waiting:
            if not waiter.done():
                waiter.set_result(None)

    def _submit(self, batch: list[Row]) -> asyncio.Future[list[int | None]]:
        """Check `batch` and queue it — or, with no log, deliver it now.

        NO AWAIT. One step that `send`, `send_many` and a pipelined publisher
        connection all take, so the order rows are submitted in is the order
        they are committed and delivered in. Returns what resolves to their
        offsets once they are durable and fanned out; raises here, with
        nothing queued, for a row the schema refuses.

        ONE stamp for the batch, not one per row: a `send_many` is a single
        transaction and its rows commit together, so per-row values would
        imply a precision the commit does not have.
        """
        if self._retirement is not None:
            raise StreamRetired(
                self._name, self._retirement.at, self._retirement.end_offset
            )

        now = time.time_ns()
        codec = self._codec
        if codec.check is not None:
            # Every row, before anything is queued: one bad map refuses the
            # batch — a map sent as pairs would be stored, and then replayed
            # as a different frame.
            for row in batch:
                codec.check(row)

        if self._validate is not None:
            # A live-only stream with a schema: refused as `append` would,
            # before anything is fanned out.
            for row in batch:
                self._validate(row)

        if self._writer is not None:
            return self._commit(batch, now)  # ty: ignore[invalid-return-type]

        # Live-only: nothing to commit, so the delivery is this one step.
        self._stamp(now)
        ts = self._wire_ts(now)
        for row in batch:
            wire = row if codec.outbound is None else codec.outbound(row)
            self._fan_out(row, encode(None, ts, wire, self._columns))

        done: asyncio.Future[list[int | None]] = (
            asyncio.get_running_loop().create_future()
        )
        done.set_result([None] * len(batch))
        return done

    def _commit(self, rows: list[Row], now: int) -> asyncio.Future[list[int]]:
        """Check `rows`, queue them for the writer, and return what resolves
        to their offsets once they are committed AND delivered. No await:
        validating and queueing are one step, so two senders queue in the
        order they called."""
        writer = self._writer
        check = self._check_stored
        assert writer is not None  # only called with a log
        assert check is not None
        stored = [self._stamp_row(row, now) for row in rows] if self._stamped else rows
        for row in stored:
            check(row)

        done: asyncio.Future[list[int]] = asyncio.get_running_loop().create_future()
        writer.submit(Job(rows=rows, stored=stored, now=now, done=done))
        self._queued += len(rows)
        return done

    def _deliver(self, jobs: list[Job], offsets: list[int]) -> None:
        """Commits, back from the writer in commit order: advance the frontier
        and fan out. NO AWAIT — see the module docstring.

        One call per commit, which may be several `send`s grouped into one
        transaction; each job's rows are adjacent in `offsets`, in queue order.
        """
        codec = self._codec
        start = 0
        for job in jobs:
            mine = offsets[start : start + len(job.rows)]
            start += len(job.rows)
            self._end_offset = mine[-1] + 1
            self._stamp(job.now)
            ts = self._wire_ts(job.now)
            for offset, row in zip(mine, job.rows, strict=True):
                wire = row if codec.outbound is None else codec.outbound(row)
                self._fan_out(row, encode(offset, ts, wire, self._columns))

            # A sender that gave up waiting has still had its rows committed
            # and delivered; there is just no one to tell.
            if not job.done.done():
                job.done.set_result(mine)

        self._drained(start)

    def _fail(self, jobs: list[Job], exc: BaseException) -> None:
        """A commit that raised: nothing landed, so nothing is delivered."""
        for job in jobs:
            if not job.done.done():
                job.done.set_exception(exc)

        self._drained(sum(len(job.rows) for job in jobs))

    def _stamp(self, now: int) -> None:
        """Record that a send happened at `now`, in epoch nanoseconds. No await.

        `now` is the same wall-clock reading `_stamp_row` stored, so the
        stats' `last_send_ts` and the log's last `streamcast_ts` agree rather
        than differing by the length of the append.

        On the hot path by necessity — nothing can know afterwards when the
        last row arrived. Measured at 42 ns per clock against 2.0 ms for a
        durable `send` and 316 ns for a live-only one with no subscribers
        attached, which is the least realistic case: a stream with no log and
        nobody listening is not doing anything.
        """
        self._last_send = time.monotonic()
        self._last_send_ts = now / 1e9

    def _wire_ts(self, now: int) -> int | None:
        """The `ts` a frame for a row sent at `now` carries. No await.

        What the log stores, so a replay sends what the live frame did (I10):
        microseconds, truncated as `_stamp_row` truncates. A stream with no log
        sends it too — the server took the row then whether or not it kept it
        — and a log created before `streamcast_ts` existed sends `null`, since
        that is all its replay could send.
        """
        if self._log is not None and not self._stamped:
            return None

        return now // 1_000

    @staticmethod
    def _stamp_row(row: Row, now: int) -> Row:
        """`row` with `streamcast_ts` added, for the log. A copy, never `row`.

        A copy because the caller's dict is theirs, and because the fan-out
        and the `where=` predicate read the original: the stamp is stored as a
        column and sent by position, never as a key in the row (I6).

        **A row that already carries the column is refused**, not overwritten.
        It is the server's to fill, and a publisher that sent one would
        otherwise learn nothing about why its value never appeared — the same
        answer litelink gives for `litelink_offset`.
        """
        if _log.STAMP in row:
            msg = f"{_log.STAMP!r} is stamped by the server; a row cannot supply it"
            raise ValueError(msg)

        return {**row, _log.STAMP: now // 1_000}

    @property
    def stats(self) -> Stats:
        """This stream's facts, as of now. No verdict — see `_stats`.

        A property rather than a method because it reads counters this object
        already holds: no log query, no socket, nothing that can fail or
        block. Poll it as often as you like.
        """
        last = self._last_send
        return Stats(
            name=self._name,
            durable=self._log is not None,
            end_offset=self._end_offset,
            subscribers=len(self._subscribers),
            started_ts=self._started_ts,
            uptime_s=time.monotonic() - self._started,
            last_send_ts=self._last_send_ts,
            last_send_age_s=None if last is None else time.monotonic() - last,
        )

    def _fan_out(self, row: Row, frame: bytes) -> None:
        """One encoded frame into every subscriber's queue that wants it.

        Encoded once by the caller and shared, so a queue entry is a pointer
        rather than a copy — which stays true with `where=` in play, because a
        filter decides whether to enqueue the shared frame rather than what to
        build. The row travels alongside it for the predicate to read.

        `offer` cannot block, raise, or detach anything, which is what makes
        iterating the set here safe without a snapshot.
        """
        for subscriber in self._subscribers:
            subscriber.offer(row, frame)

    async def aclose(self, reason: str = "server shutting down") -> None:
        """Drop every subscriber with a 1001. Does not close the log.

        Concurrently, because `close` waits for each peer's close handshake
        and doing that in series is `close_timeout` per dead connection.
        """
        await asyncio.gather(
            *(
                subscriber.close(CloseCode.GOING_AWAY, reason)
                for subscriber in list(self._subscribers)
            ),
            return_exceptions=True,
        )

        # Whatever is queued is committed and delivered first, then the thread
        # stops — before the log it writes to can be closed underneath it.
        if self._writer is not None:
            await asyncio.to_thread(self._writer.close)

        # A log this object opened is a log this object closes. One handed in
        # is left alone: the caller may be sharing it, and closing a borrowed
        # handle is how a library becomes one you cannot lend to.
        if self._owned is not None:
            self._owned.close()
            self._owned = None

    # -- subscribe ---------------------------------------------------------

    async def serve_publisher(self, connection: Peer) -> None:
        """Take rows from a remote publisher and commit them as this process.

        **The whole point is that this adds no authority.** A publisher hands
        over rows; `send` and `send_many` are the same calls a local publisher
        makes, on the same handle, in the same process. litelink allows one
        writer per log and that writer is still this server — which is what
        makes remote publishing safe where a second `WriteHandle` on another
        box is not (litelink refuses neither, and cannot detect one).

        I1 is what makes concurrency free here. Each send is checked and
        queued for the stream's writer in one step, committed in that order,
        and delivered in commit order, so two handlers cannot interleave:
        `send_many` stays one transaction, and a batch's offsets are adjacent
        even with another publisher racing it.

        **A frame is a row, or a list of rows**, and the publisher chooses
        which — exactly the choice a local publisher makes between `send` and
        `send_many`, with the same consequences. The reply is the offsets
        assigned, so a publisher learns its rows are durable the way an
        `await send(...)` does.

        **Pipelined.** Each frame is queued for the writer as it is read,
        without waiting for the one before it to commit, and answered in the
        order it arrived. At most `max_in_flight` replies are owed one
        connection; past that it stops being read, which is backpressure at
        the socket, not an error.

        A row the schema refuses is answered and the connection stays open,
        because that is what the local call does: `send` raises, the caller
        catches it, and the next call works. Closing would make one bad row
        cost every good one behind it.

        **A retired stream refuses publishers**, with 4410 (`Close.RETIRED`):
        its log is open read-only and takes nothing. The stream decides, from
        what it is; subscribers still read its history.
        """
        if self._retirement is not None:
            await connection.close(
                Close.RETIRED,
                refusal(
                    "stream_retired",
                    at=self._retirement.at,
                    end_offset=self._retirement.end_offset,
                ),
            )
            return

        await connection.send(
            greeting(
                stream=self._name,
                end_offset=self._end_offset,
                replay=None,
                durable=self._log is not None,
                group_commit=self._group_commit,
                schema=self._shape,
                metadata=self.metadata_uri,
                stream_id=self._stream_id,
            )
        )

        # **Pipelined.** Each frame is checked and queued for the writer the
        # moment it is read — without waiting for the one before it to commit
        # — and its reply follows in a task of its own, in frame order. So a
        # publisher with several sends in flight keeps the writer fed, and
        # its rows group into one commit rather than waiting a round trip
        # each. The writer commits in queue order, so replies leave in the
        # order frames arrived, and a refusal takes its place in that line.
        # Bounded: a client that ignores its own window is held at the socket.
        replies: asyncio.Queue[asyncio.Future[list[int | None]] | str | None] = (
            asyncio.Queue(maxsize=self._max_in_flight)
        )
        replying = asyncio.create_task(self._reply(connection, replies))
        try:
            async for frame in connection:
                try:
                    rows = decode_publish(frame)
                except ProtocolError as exc:
                    await replies.put(publish_error("bad_frame", detail=str(exc)))
                    continue

                try:
                    # **Text to bytes, before anything else.** A publisher over
                    # JSON can only send a binary value as text, in its
                    # column's encoding, and litelink refuses a `str` for a
                    # binary column.
                    inbound = self._codec.inbound
                    if inbound is not None:
                        rows = (
                            [inbound(row) for row in rows]
                            if isinstance(rows, list)
                            else inbound(rows)
                        )

                    batch: list[Row] = list(rows) if isinstance(rows, list) else [rows]
                    # Room first: at `max_inbound` this stops reading the
                    # socket, and TCP holds the publisher back.
                    await self._room(len(batch))
                    queued = self._submit(batch) if batch else None

                except (ValueError, TypeError) as exc:
                    # litelink names the column and what it found. Passed
                    # through rather than summarised: a publisher debugging a
                    # schema mismatch needs the column name more than it
                    # needs a tidy sentence.
                    await replies.put(publish_error("rejected", detail=str(exc)))
                    continue

                await replies.put(queued if queued is not None else publish_ack([]))

        finally:
            # Every row read was queued and will commit; the replies still owed
            # go out if the connection lets them.
            await replies.put(None)
            await replying

    @staticmethod
    async def _reply(
        connection: Peer,
        replies: asyncio.Queue[asyncio.Future[list[int | None]] | str | None],
    ) -> None:
        """Send each queued frame's reply, in frame order, until told to stop."""
        while (item := await replies.get()) is not None:
            if isinstance(item, str):
                reply = item
            else:
                try:
                    reply = publish_ack(await item)
                except Exception as exc:  # noqa: BLE001 — answered, not raised
                    # The commit itself failed — the disk, SQLite — so nothing
                    # landed. Answered for this frame alone; dying here would
                    # leave the reader blocked on a queue nothing drains.
                    reply = publish_error("commit_failed", detail=str(exc))

            with contextlib.suppress(ConnectionClosed):
                await connection.send(reply)

    async def serve_subscriber(
        self,
        connection: Peer,
        requested: int | None,
        where: Where | None = None,
    ) -> None:
        """Attach one subscriber and serve it until the connection ends.

        Raises `NotReplayable` for an offset this stream cannot serve, which
        `serve` turns into a 4416, and `ValueError` for a `where=` it cannot,
        which becomes a 4400. Everything else — the greeting, the replay, the
        live pump — happens here, and the order is load-bearing.

        **`where=` filters both halves through the same compiled predicate.**
        The replay and the live queue are one stream to a subscriber, so a
        filter applied to only one of them would make a resume deliver what
        the live connection never would. `_log.rows` already yields dicts, so
        there is no second implementation and nothing to diverge — and see
        `_log.replay` for why the first replayed row is checked before it is
        filtered.
        """
        predicate = None
        if where is not None:
            # Validated and compiled BEFORE anything is sent, so a filter
            # naming a column this stream does not have is a refusal at
            # subscribe rather than a subscription that never delivers.
            _filter.validate(where, self._columns)
            predicate = _filter.compile_where(_filter.prepare(where, self._declared))

        # `(log, start)` rather than `start`, so the handle a replay needs
        # travels with the decision that it is needed. The alternative is
        # re-narrowing `self._log` at the use site, which is an assertion
        # about a branch three statements away.
        resolved = await self._resolve(requested)

        subscriber = Subscriber(
            connection, max_backlog=self._max_backlog, where=predicate
        )
        # ── ATOMIC. Do not put an await between these two statements. ──
        # Joining the set first means nothing sent from here on is missed;
        # reading the frontier second means everything below it is already
        # durable. See the module docstring.
        self._subscribers.add(subscriber)
        frontier = self._end_offset
        # ──────────────────────────────────────────────────────────────

        replay: AsyncGenerator[tuple[int, bytes], None] | None = None
        replaying: tuple[int, int] | None = None
        try:
            if resolved is not None:
                if frontier is None:  # pragma: no cover — implied by `resolved`
                    # A resolved replay means `_resolve` found a log, and a
                    # log means the counter is an integer. Stated as a raise
                    # rather than an assert because the alternative — skipping
                    # the replay — would be a silent gap at the join.
                    msg = "a replay resolved against a stream with no offsets"
                    raise RuntimeError(msg)

                replaying = (resolved[1], frontier)
                replay = await self._replay_from(*resolved, frontier, predicate)

            await connection.send(
                greeting(
                    stream=self._name,
                    end_offset=frontier,
                    replay=replaying,
                    durable=self._log is not None,
                    group_commit=self._group_commit,
                    schema=self._shape,
                    # Where the history is read, and whose: a subscriber
                    # checks the id against the file, so a file at the same
                    # path that belongs to another stream is refused.
                    metadata=self.metadata_uri,
                    stream_id=self._stream_id,
                    where=dict(where) if where is not None else None,
                )
            )
            await subscriber.run(replay)
        finally:
            self._subscribers.discard(subscriber)
            if replay is not None:
                # Releases the DuckDB result the scan is holding. A subscriber
                # that disconnects mid-replay leaves this generator suspended
                # otherwise, and under a reconnect storm that is an unbounded
                # number of live scans against one log.
                await replay.aclose()

    async def _resolve(self, requested: int | None) -> tuple[LogHandle, int] | None:
        """The log and offset a replay should start at, or None for live-only.

        Every refusal here is cheap and answerable before a scan is opened.
        The one that is not — an offset below what the log still holds — is
        settled in `_replay_from`, because only the scan knows.
        """
        if requested is None:
            return None

        log = self._log
        if log is None:
            raise NotReplayable("not_durable")

        frontier = self._end_offset
        if frontier is None:  # pragma: no cover — a log always has a counter
            msg = "a stream with a log has no offset counter"
            raise RuntimeError(msg)

        if requested == EARLIEST:
            # The only call that asks the log where it starts, and it is in a
            # thread because `coverage()` resolves offset extents from table
            # statistics — local file reads, and a metadata GET when the log
            # has been evicted to its published table.
            first = await asyncio.to_thread(
                partial(_log.earliest, log, published=self._replay_published)
            )
            if first is None and self._floor is not None:
                # A migrated stream's new log is empty until its first send,
                # and the stream is not: its history is in the retired logs.
                # The earliest THIS server serves is where the current log
                # begins, so that is what EARLIEST means here.
                first = self._floor

            elif first is None:
                # **Nothing to replay: EARLIEST starts at the frontier** — where
                # a never-written log's first row will land, and all a log
                # evicted dry can still serve ("everything the stream can
                # still serve"; its history is a catch-up's to read). Not a
                # refusal: a refusal made the client subscribe again, "from
                # now", and any row committed between the two was in neither
                # — replayed by nothing, sent live to nobody. `frontier` was
                # read before the await above, so a row committed since is at
                # or above it, and the replay to the frontier at attach (I2)
                # carries it.
                first = frontier

            requested = first

        if self._floor is not None and requested < self._floor:
            # **Below the seam is a retired log, which this server does not
            # read.** Refused here rather than left to the scan, because the
            # scan cannot see it: the current log holds nothing below its
            # start, so an empty replay would pass for "nothing outstanding"
            # and the subscriber would receive a stream with the old log's
            # tail silently missing — invariant 4's hole at the join.
            raise NotReplayable("evicted", offset=requested, earliest=self._floor)

        if requested > frontier:
            raise NotReplayable("ahead", offset=requested, end_offset=frontier)

        behind = frontier - requested
        # **None means no bound, and `too_old` then never fires.** The
        # request is served from wherever the log can serve it, which on a
        # log opened with `replay_published=True` is the whole history out of
        # object storage. That is the setting that makes a plain WebSocket
        # client in any language able to replay a stream from the beginning
        # with no litelink, no credentials and no dependency on this repo —
        # see `Stream.new` for what it costs.
        if self._max_replay is not None and behind > self._max_replay:
            # **The offset distance said too far. Ask what it actually costs.**
            # `max_replay` bounds the work a replay does, and that work is
            # rows — offset distance is a proxy, exact only while the offset
            # space is dense. It is not: a `restore` fences 2**20 offsets
            # that were never issued (2**40 with no WAL replica), so a consumer 150 rows behind a
            # failed-over producer measures as a million and is refused a
            # replay the server could serve instantly.
            #
            # Only reached when the free check has already failed, so an
            # ordinary subscribe never pays for it — and a subscribe about to
            # be REFUSED can afford 30 ms to find out whether it should be.
            behind = await asyncio.to_thread(
                partial(
                    _log.rows_from, log, requested, published=self._replay_published
                )
            )

        if self._max_replay is not None and behind > self._max_replay:
            raise NotReplayable(
                "too_old", offset=requested, behind=behind, max_replay=self._max_replay
            )

        return log, requested

    async def _replay_from(
        self,
        log: LogHandle,
        start: int,
        frontier: int,
        where: Predicate | None = None,
    ) -> AsyncGenerator[tuple[int, bytes], None]:
        """The replay stream, with its first row checked against the request.

        **Pulled one row early on purpose.** A log whose retention has passed
        the requested offset would otherwise serve from wherever it does
        start, and the subscriber would receive a stream that silently begins
        above where it asked — a hole at the join, which is the one wrong
        answer a resume must never give. Reading the first row before the
        greeting turns that into a 4416 with the offset that would have
        worked.

        An empty range is not a hole: `start == frontier` is a subscriber
        resuming with nothing outstanding, and it is the common case for a
        reconnect that lost the connection rather than the race.
        """
        stream = _log.replay(
            log,
            start,
            frontier,
            where,
            self._codec.outbound,
            published=self._replay_published,
        )
        # A log evicted below `start` holds nothing local there, and a local
        # read leaves those rows out rather than refusing; the first row's
        # offset is then above the request, and is refused just below.
        first = await anext(stream, None)
        if first is None:
            # **Nothing local in `[start, frontier)`: two causes, one a hole.**
            # A log evicted below `start` holds those rows in its published
            # table only, and a local replay leaves them out rather than
            # refusing — serving that as an empty replay would hand the
            # subscriber a stream silently missing them (invariant 4). A
            # restored log's fence is the other cause: offsets that were never
            # issued, where an empty replay is the right answer. The
            # published table tells them apart. A rare path, so `coverage()`
            # may read its manifests.
            if start < frontier and not self._replay_published:
                held = (await asyncio.to_thread(log.coverage)).published
                if held is not None and held[1] > start:
                    first_local = await asyncio.to_thread(
                        partial(_log.earliest, log, published=False)
                    )
                    raise NotReplayable(
                        "evicted",
                        offset=start,
                        earliest=frontier if first_local is None else first_local,
                    )

            return _empty()

        offset, _frame = first
        if offset > start:
            await stream.aclose()
            raise NotReplayable("evicted", offset=start, earliest=offset)

        # Decoded from the frame, so a binary column is text again — and the
        # filter compares bytes. `inbound` turns it back, as the client does.
        message = decode(first[1])[2]
        if self._codec.inbound is not None:
            message = self._codec.inbound(message)

        if where is not None and not where(message):
            # `_log.replay` yields its first row whatever the filter says, so
            # the check above measures the LOG's floor rather than the first
            # match. Having used it for that, drop it — the subscriber asked
            # not to be sent this row.
            #
            # Decoded rather than threaded back out of `_log.replay` as a
            # dict: it is ONE row per subscribe, 386 ns, against a second
            # return shape on the hot replay path.
            return stream

        return _prepend(first, stream)


def _open_retired(
    root: str | PathLike[str], name: str, *, schema: Mapping[str, object]
) -> tuple[LogHandle, _metadata.Metadata] | None:
    """The retired stream's log opened read-only, and its metadata — or None
    if the stream is not retired. The declared schema is checked as for a
    live stream: a declaration that disagreed would be silently ignored."""
    metadata = _metadata.load(root, name)
    if metadata is None or metadata.retirement is None:
        return None

    declared = _declaration(schema)
    log = litelink.open(root, metadata.current.name, read_only=True)
    if list(_log.declared(log.schema)) != list(declared):
        found = list(_log.columns(log))
        log.close()
        msg = (
            f"the retired stream {name!r} has columns {found}, and this call "
            f"declares {declared.names}; declare the columns it has"
        )
        raise ValueError(msg)

    return log, metadata


def _open_or_create(
    root: str | PathLike[str],
    name: str,
    *,
    schema: Mapping[str, object],
    sort_by: Sequence[str] | None,
    config: object | None,
    published: str | None,
    s3_options: object | None,
) -> tuple[WriteHandle, _metadata.Metadata | None]:
    """The log for this stream, created if it is not there yet, and its metadata.

    The try/except every caller writes identically: a server has to `new` the
    first time and `open` every time after, and `new` raises rather than
    adopting an existing log. Doing it here is most of what makes `root=` +
    `schema=` worth having.

    **An existing log is checked against the declared schema**, because `open`
    takes none of the shape — it reads it from disk — so a declaration that
    disagreed would be silently ignored and every send would be validated
    against columns the caller did not write down. That is the failure this
    convenience would otherwise introduce.
    """
    declared = _declaration(schema)
    # A migrated stream is written to its metadata's CURRENT log, whose name
    # is not the stream's. No metadata file means it has not been served by
    # this version yet, and so has never migrated: one log, at its name.
    metadata = _metadata.load(root, name)
    current = name if metadata is None else metadata.current.name
    try:
        log = litelink.open(root, current)
    except FileNotFoundError:
        if metadata is not None:
            msg = (
                f"the metadata at {_metadata.path(root, name)} names {current!r} "
                f"as the current log, and there is no log at {root}/{current}"
            )
            raise FileNotFoundError(msg) from None

        return litelink.new(
            root,
            name,
            schema=_log.with_system(declared),
            sort_by=sort_by,
            config=config,  # ty: ignore[invalid-argument-type]
            published=published,
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
        ), None

    # Compared WITHOUT `streamcast_ts`, so a log created before the column
    # existed still opens: it keeps its shape and is simply never stamped. A
    # log's columns are fixed for its life; `Stream.migrate` is how a stream
    # gains one.
    if list(_log.declared(log.schema)) != list(declared):
        # Read BEFORE closing. `log.schema` goes to the buffer's `meta` table,
        # so building this message after `close()` raises "Cannot operate on a
        # closed database" and buries the real complaint.
        found = list(_log.columns(log))
        log.close()
        msg = (
            f"the log at {root}/{current} has columns {found}, and this "
            f"stream declares {declared.names}. litelink fixes a log's shape at "
            f"creation, so an existing one cannot be re-declared. To change the "
            f"schema, stop the server and use Stream.migrate; to keep it, "
            f"declare the columns the log has."
        )
        raise ValueError(msg)

    return log, metadata


def _declaration(schema: Mapping[str, object]) -> pa.Schema:
    """A declaration as Arrow, refusing the one name streamcast owns."""
    declared = _schema.to_arrow(schema)
    reserved = [name for name in declared.names if name in _log.SYSTEM]
    if reserved:
        msg = (
            f"{', '.join(map(repr, reserved))} is a column streamcast owns and "
            f"fills itself; declare the stream's own under another name"
        )
        raise ValueError(msg)

    return declared


def _floor(metadata: _metadata.Metadata | None) -> int | None:
    """Where a migrated stream's live log starts; None if it never migrated.

    None for a stream with no sealed log even though its metadata knows a
    start: below it there is nothing to point a subscriber at, and an empty
    log's `EARLIEST` starts at its first row.
    """
    if metadata is None or not metadata.sealed_logs:
        return None

    return metadata.live_log.start_offset


def _retired(
    root: str | PathLike[str], metadata: _metadata.Metadata | None
) -> tuple[tuple[Path, str], ...]:
    """The retired logs still on THIS disk, as `(root, name)`.

    Only those present: a restored stream has its current log and nothing
    else, and a maintainer asked to open a log that is not there would fail
    every pass.
    """
    if metadata is None:
        return ()

    return tuple(
        (Path(root), entry.name)
        for entry in metadata.retired
        if (Path(root) / entry.name).is_dir()
    )


def _seal_and_succeed(
    old: LogHandle,
    root: str | PathLike[str],
    name: str,
    *,
    declared: pa.Schema,
    sort_by: Sequence[str] | None,
    config: object | None,
    s3_options: object | None,
    retired: bool = False,
) -> tuple[WriteHandle, TierStatistics]:
    """Seal `old` for good, then create the log that follows it.

    Returns the new log and `old`'s statistics (`column_statistics()`, the
    whole log). Read here — after the seal and the push, before anything new
    exists — so a failure to read them leaves the stream exactly as it was.

    The seal comes FIRST and entirely: a failure there — a published location that is
    unreachable — leaves the stream exactly as it was, one log, current, with
    its rows merely sealed earlier than the maintainer would have.
    """
    # litelink's own end-of-life: every buffered row sealed, everything
    # published IN FULL, the staging table evicted, and the log refusing any
    # append from here — from this handle or any that opens it later. It is
    # resumable, so a migration that died partway finishes it on the rerun.
    if retired:
        pass  # a rerun after a migration that died once the log was retired
    elif not isinstance(
        old, litelink.WriteHandle
    ):  # pragma: no cover — opened as a writer
        msg = f"{old.name} must be opened as a writer to retire it"
        raise TypeError(msg)
    elif old.config.wal_replication:
        with _replicate.retiring(old):
            old.retire()
    else:
        old.retire()

    # The whole log (`tier=None`). After `retire` everything is in the
    # published table, so this is the same answer `tier="published"` would
    # give — but None is the one that means "the whole log" whatever litelink
    # later does with tiers.
    statistics = old.column_statistics()

    start = old.end_offset()
    try:
        created = litelink.new(
            root,
            name,
            schema=_log.with_system(declared),
            sort_by=old.sort_by if sort_by is None else sort_by,
            config=old.config if config is None else config,  # ty: ignore[invalid-argument-type]
            # The same published location, unless it was litelink's local
            # default, which lives INSIDE the old log's directory: the new log
            # gets its own default rather than a table nested in a retired log.
            published=None if _metadata.default_published(old) else old.published,
            s3_options=s3_options,  # ty: ignore[invalid-argument-type]
            start_offset=start,
        )
    except FileExistsError:
        # **An orphan from a migration that died before its metadata was
        # saved.** Adopted only if it is exactly what this call would have
        # made and nothing has written to it; anything else is a log with
        # rows the metadata does not account for, which is not a decision to
        # make on the caller's behalf.
        orphan = litelink.open(root, name)
        if (
            orphan.end_offset() == start
            and _log.lowest(orphan) is None
            and list(_log.declared(orphan.schema)) == list(declared)
            and _log.is_current(orphan)
        ):
            return orphan, statistics

        orphan.close()
        msg = (
            f"a log already exists at {root}/{name} and the metadata does not "
            f"name it. It is not an empty log of the requested shape starting "
            f"at {start}, so it was not adopted — inspect it, and remove it if "
            f"it is the remains of an abandoned migration."
        )
        raise FileExistsError(msg) from None

    return created, statistics


async def _empty() -> AsyncGenerator[tuple[int, bytes], None]:
    """Nothing to replay. A generator rather than None so the pump has one path."""
    return
    yield  # pragma: no cover — unreachable, and what makes this a generator


async def _prepend(
    first: tuple[int, bytes], rest: AsyncGenerator[tuple[int, bytes], None]
) -> AsyncGenerator[tuple[int, bytes], None]:
    """Put back the row `_replay_from` pulled to inspect it."""
    yield first
    async for item in rest:
        yield item


__all__ = ["MAX_REPLAY", "Stream"]
