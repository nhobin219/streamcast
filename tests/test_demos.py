"""`just demo`: the name each runnable demo goes by, and the book demo's parts.

The demos themselves run against live public feeds, so what is tested here is
everything that does not need one: that every name maps to a module that
exists, and that the order book's conversion and page are what the browser
expects.
"""

from __future__ import annotations

import asyncio
import importlib.util
import urllib.request

import pytest
import websockets

import streamcast
from examples import __main__ as demos
from examples.keyed_table import book


class TestTheNames:
    @pytest.mark.parametrize("name", sorted(demos.DEMOS))
    def test_each_name_runs_a_module_that_exists(self, name):
        module, _, _ = demos.DEMOS[name]
        assert importlib.util.find_spec(module) is not None

    @pytest.mark.parametrize("flag", ["--list", "-l", "list"])
    def test_the_list_names_every_demo(self, flag, capsys):
        demos.main([flag])
        listed = capsys.readouterr().out
        assert all(f"  {name} " in listed for name in demos.DEMOS)

    def test_an_unknown_name_is_refused_with_the_list(self, capsys):
        with pytest.raises(SystemExit) as refused:
            demos.main(["nope"])

        assert refused.value.code == 2
        assert "no demo called 'nope'" in capsys.readouterr().err

    def test_flags_without_a_name_go_to_the_server(self, monkeypatch):
        ran: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            demos.runpy,
            "run_module",
            lambda module, **_: ran.append((module, demos.sys.argv[1:])),
        )
        demos.main(["--no-log"])
        assert ran == [("examples.server", ["--no-log"])]


class TestTheBook:
    @pytest.mark.parametrize(
        ("event", "deleted", "side"),
        [
            ("order_created", False, "buy"),
            ("order_changed", False, "buy"),
            ("order_deleted", True, None),
        ],
    )
    def test_each_feed_event_is_a_keyed_row(self, event, deleted, side):
        order = {
            "id": 2056202981134337,
            "order_type": 0,
            "price": 84147.02,
            "amount": 0.125,
            "microtimestamp": "1790837655598000",
        }
        row = book.row(event, order)
        assert row["order_id"] == 2056202981134337
        assert row["deleted"] is deleted
        assert row["side"] == side
        assert (row["price"] is None) is deleted

    async def test_the_page_and_the_stream_share_one_port(self, tmp_path):
        stream = streamcast.Stream.new("orders", root=tmp_path, schema=book.SCHEMA)
        async with streamcast.serve(
            stream, "127.0.0.1", 0, process_request=book.page, maintain=False
        ) as server:
            port = server.sockets[0].getsockname()[1]

            def get() -> tuple[str, str]:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/") as response:  # noqa: S310
                    return response.headers["Content-Type"], response.read().decode()

            # On a thread: the server answers on this test's event loop.
            content_type, body = await asyncio.to_thread(get)
            assert content_type == "text/html; charset=utf-8"
            assert "createGrid" in body

            await stream.send(
                book.row("order_deleted", {"id": 1, "microtimestamp": "1"})
            )
            async with websockets.connect(
                f"ws://127.0.0.1:{port}/orders?offset=0"
            ) as ws:
                await ws.recv()  # the greeting
                assert '"order_id":1' in str(await ws.recv())
