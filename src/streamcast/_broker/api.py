"""HTTP control and WebSocket transport over the same lifespan runtime."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
from contextlib import asynccontextmanager
from typing import Any, Final

from fastapi import FastAPI, Request, WebSocket
from starlette.responses import JSONResponse, Response
from websockets.exceptions import ConnectionClosed

from streamcast import _stats
from streamcast._asgi import _Peer, _relative
from streamcast._broker.model import CONTROL, NAME, BrokerConfig, Fault
from streamcast._broker.runtime import BrokerRuntime
from streamcast._errors import Close, NotReplayable, ProtocolError
from streamcast._protocol import ACCEPTED, VERSION, Publish, parse_subscribe, refusal
from streamcast._tasks import join

DETAIL_CHARS: Final = 80
"""How much of a parse or filter error travels in a 4400.

`refusal` trims by dropping whole fields, so an untruncated detail would push
itself off the close frame and the subscriber would get `{"error":
"bad_request"}` — the one case where saying less loses the entire diagnosis.
Eighty characters is every message `parse_subscribe` produces.
"""


def error(fault: Fault) -> JSONResponse:
    headers = {}
    if fault.status == 401:
        headers["WWW-Authenticate"] = 'Basic realm="streamcast", charset="UTF-8"'
    elif fault.status in (429, 503):
        headers["Retry-After"] = "1"

    return JSONResponse(
        {
            "error": {
                "code": fault.code,
                "message": fault.code.replace("_", " "),
                "retryable": fault.status in (429, 503),
                "operation_id": None,
            }
        },
        status_code=fault.status,
        headers=headers,
    )


async def authorize(config: BrokerConfig, scope, action: str, resource: str) -> str:
    principal = await config.authorization.authenticate(scope)
    if principal is None:
        raise Fault(401, "authentication_required")

    if not await config.authorization.allows(principal, action, resource):
        raise Fault(403, "forbidden")

    return principal


async def body(request: Request, maximum: int) -> Any:
    if (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
    ):
        raise Fault(415, "json_required")

    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > maximum:
            raise Fault(413, "body_too_large")

        raw.extend(chunk)

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError

            result[key] = value

        return result

    try:
        return json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (ValueError, UnicodeError, RecursionError):
        raise Fault(400, "invalid_json") from None


def application(config: BrokerConfig, streams=()) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        # The application owns startup too. ASGI cancellation cannot discard a
        # just-opened registry/handle before its owner has a chance to close it.
        opening = asyncio.create_task(BrokerRuntime.open(config, streams=streams))
        cancelled = await join(opening)
        runtime = opening.result()
        if cancelled:
            closing = asyncio.create_task(runtime.close())
            await join(closing)
            closing.result()
            raise asyncio.CancelledError

        app.state.broker = runtime
        try:
            yield
        finally:
            closing = asyncio.create_task(runtime.close())
            cancelled = await join(closing)
            closing.result()
            if cancelled:
                raise asyncio.CancelledError

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.exception_handler(Fault)
    async def fault_handler(_request: Request, fault: Fault):
        return error(fault)

    @app.exception_handler(Exception)
    async def internal_handler(_request: Request, _fault: Exception):
        return error(Fault(500, "internal_error"))

    def runtime() -> BrokerRuntime:
        value = getattr(app.state, "broker", None)
        if value is None or value.closing:
            raise Fault(503, "broker_unavailable")

        return value

    @app.get(CONTROL)
    async def capabilities(request: Request):
        await authorize(config, request.scope, "control.read", "*")
        owner = runtime()
        return {
            "version": 1,
            "broker_id": config.broker_id,
            "boot_id": owner.boot_id,
            "transport_max_bytes": config.transport_max_bytes,
            "control_max_bytes": config.control_max_bytes,
            "max_pending": config.max_pending,
            "storage_profiles": {
                name: profile.public() for name, profile in config.profiles.items()
            },
            "history_prefix_protected": sorted(config.history_guards),
            "wire_versions": {"write": VERSION, "read": list(ACCEPTED)},
        }

    @app.get(CONTROL + "/streams")
    async def listing(request: Request):
        principal = await authorize(config, request.scope, "control.read", "*")
        owner = runtime()
        try:
            limit = int(request.query_params.get("limit", "100"))
            if not 1 <= limit <= 1000 or set(request.query_params) - {
                "limit",
                "cursor",
            }:
                raise ValueError

            after = ""
            cursor = request.query_params.get("cursor")
            if cursor is not None:
                revision, after = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                if type(revision) is not int or not isinstance(after, str):
                    raise ValueError

                if revision != owner.collection:
                    raise Fault(409, "collection_changed")

        except (ValueError, TypeError, UnicodeError):
            raise Fault(400, "invalid_pagination") from None

        # Authorization can await; snapshot the collection before it does.
        revision = owner.collection
        names = [name for name in sorted(owner.records) if name > after]
        visible = []
        for name in names:
            if await config.authorization.allows(principal, "control.read", name):
                visible.append(name)

        if owner.collection != revision:
            raise Fault(409, "collection_changed")

        selected = visible[:limit]
        next_cursor = None
        if len(visible) > limit:
            next_cursor = base64.urlsafe_b64encode(
                json.dumps([revision, selected[-1]]).encode()
            ).decode()

        return {
            "revision": revision,
            "streams": [owner.view(name) for name in selected],
            "next_cursor": next_cursor,
        }

    @app.get(CONTROL + "/streams/{name}")
    async def get_stream(name: str, request: Request):
        await authorize(config, request.scope, "control.read", name)
        value = runtime().view(name)
        return JSONResponse(value, headers={"ETag": f'"{value["revision"]}"'})

    @app.get(CONTROL + "/operations/{operation_id}")
    async def operation(operation_id: str, request: Request):
        principal = await authorize(config, request.scope, "control.read", "*")
        value = runtime().operations.get(operation_id)
        if value is None:
            raise Fault(404, "unknown_operation")

        if not await config.authorization.allows(
            principal, "control.read", value["name"]
        ):
            raise Fault(403, "forbidden")

        return value

    async def mutation(name: str, request: Request, kind: str):
        action = "control.migrate" if kind == "migration" else "control.mutate"
        principal = await authorize(config, request.scope, action, name)
        owner = runtime()
        key = request.headers.get("idempotency-key", "")
        if not key:
            raise Fault(428, "idempotency_key_required")

        if not 1 <= len(key) <= 128 or any(ord(c) < 33 or ord(c) > 126 for c in key):
            raise Fault(400, "invalid_idempotency_key")

        value = await body(request, config.control_max_bytes)
        # These tasks belong to the runtime, not the socket. Close drains them
        # before the operation worker and the registry are shut down.
        receipt = await owner.control(
            owner.mutate(
                principal,
                key,
                request.method,
                name,
                kind,
                value,
                request.headers.get("if-match"),
                request.headers.get("if-none-match"),
            )
        )
        return JSONResponse(
            receipt,
            status_code=receipt["status"],
            headers={
                "ETag": f'"{receipt["revision"]}"',
                "Location": (
                    f"{request.scope.get('root_path', '').rstrip('/')}"
                    f"{CONTROL}/operations/{receipt['operation_id']}"
                ),
            },
        )

    @app.put(CONTROL + "/streams/{name}")
    async def put_stream(name: str, request: Request):
        return await mutation(name, request, "definition")

    @app.put(CONTROL + "/streams/{name}/desired")
    async def desired(name: str, request: Request):
        return await mutation(name, request, "desired")

    @app.post(CONTROL + "/streams/{name}/migrations")
    async def migration(name: str, request: Request):
        return await mutation(name, request, "migration")

    @app.get(_stats.STATS_PATH if isinstance(config.stats, bool) else config.stats)
    async def stats(request: Request):
        if config.stats is False:
            raise Fault(404, "not_found")

        principal = await authorize(config, request.scope, "stats.read", "*")
        owner = runtime()
        streams = []
        for name, generation in list(owner.generations.items()):
            if generation.accepting and await config.authorization.allows(
                principal, "stats.read", name
            ):
                streams.append(generation.stream)

        return Response(_stats.payload(streams), media_type="application/json")

    @app.websocket("/{path:path}")
    async def websocket(websocket: WebSocket, path: str):
        query = websocket.scope.get("query_string", b"").decode("latin-1")
        target = _relative(websocket.scope) + ("?" + query if query else "")
        # Choose a grant without parsing or exposing stream existence first.
        action = "data.publish" if "publish" in websocket.query_params else "data.read"
        name = path
        try:
            await authorize(config, websocket.scope, action, name)
            owner = runtime()
        except Fault as fault:
            await websocket.send_denial_response(error(fault))
            return

        await websocket.accept()
        original_receive = websocket._receive

        async def bounded_receive():
            message = await original_receive()
            frame = message.get("bytes")
            if frame is None:
                text = message.get("text")
                frame = b"" if text is None else text.encode()

            if len(frame) > config.transport_max_bytes:
                await websocket.close(1009, "frame too large")
                return {"type": "websocket.disconnect", "code": 1009}

            return message

        websocket._receive = bounded_receive
        async with _Peer(websocket) as peer:
            try:
                parsed_name, requested, where = parse_subscribe(target)
                if (
                    parsed_name not in owner.static
                    and NAME.fullmatch(parsed_name) is None
                ):
                    raise ValueError("invalid stream name")

            except ValueError as exc:
                await peer.close(
                    Close.BAD_REQUEST,
                    refusal("bad_request", detail=str(exc)[:DETAIL_CHARS]),
                )
                return

            # Check the parsed capability as well; malformed/duplicate query
            # fields cannot turn a read grant into a publishing grant.
            required = "data.publish" if isinstance(requested, Publish) else "data.read"
            try:
                await authorize(config, websocket.scope, required, parsed_name)
            except Fault:
                await peer.close(1008, "forbidden")
                return

            generation = owner.generations.get(parsed_name)
            if generation is None or not generation.accepting:
                await peer.close(
                    Close.NO_SUCH_STREAM,
                    refusal(
                        "no_such_stream",
                        **(
                            {"serves": sorted(owner.static)}
                            if config.registry is None
                            else {}
                        ),
                    ),
                )
                return

            async def dispatch():
                try:
                    if isinstance(requested, Publish):
                        await generation.stream.serve_publisher(
                            peer, requested.publisher
                        )
                    else:
                        await generation.stream.serve_subscriber(peer, requested, where)
                except ConnectionClosed:
                    pass
                except ProtocolError as exc:
                    # A `where=` this stream cannot serve — a column it does
                    # not have, a value that is not a scalar. Refused, with the
                    # reason, rather than served as a subscription that never
                    # delivers, which looks exactly like a quiet stream.
                    await peer.close(
                        Close.BAD_REQUEST,
                        refusal("bad_request", detail=str(exc)[:DETAIL_CHARS]),
                    )
                except ValueError:
                    await peer.close(Close.BAD_REQUEST, refusal("bad_request"))
                except NotReplayable as exc:
                    await peer.close(
                        Close.NOT_REPLAYABLE,
                        refusal("not_replayable", why=exc.why, **exc.fields),
                    )
                except Exception:  # noqa: BLE001 -- close without exposing storage details
                    await peer.close(1011, "stream unavailable")

            async def connection():
                serving = asyncio.create_task(dispatch())
                disconnected = asyncio.create_task(peer.wait_closed())
                try:
                    await asyncio.wait(
                        [serving, disconnected], return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    serving.cancel()
                    disconnected.cancel()
                    await asyncio.gather(serving, disconnected, return_exceptions=True)

            task = asyncio.create_task(connection())
            generation.connections[task] = peer
            try:
                await asyncio.shield(task)
            finally:
                # Uvicorn may cancel handlers before lifespan shutdown. The
                # peer is closed first; its owned worker lease is joined next.
                await peer.close(1001, "connection ending")
                with contextlib.suppress(asyncio.CancelledError):
                    await task

                generation.connections.pop(task, None)

    return app
