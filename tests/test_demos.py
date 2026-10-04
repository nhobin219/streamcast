"""`just demo`: every demo's processes, the broker they share, the book's parts.

The demos run against live public feeds, so what is tested here is everything
that does not need one: that each demo is a broker plus clients made of
modules that exist, that the runner refuses a port already taken, and that
the generic broker serves what a producer publishes to a subscriber.
"""

from __future__ import annotations

import asyncio
import importlib.util
import socket
import sys

import pytest

import streamcast
from examples import __main__ as demos
from examples import broker
from examples.book import producer as book


def modules(demo: demos.Demo) -> list[str]:
    """The module each process runs, for the ones `-m` runs."""
    return [step.argv[step.argv.index("-m") + 1] for step in demo.processes]


class TestEachDemo:
    @pytest.mark.parametrize("name", sorted(demos.DEMOS))
    def test_runs_modules_that_exist(self, name):
        for module in modules(demos.DEMOS[name]):
            assert importlib.util.find_spec(module) is not None, module

    @pytest.mark.parametrize("name", sorted(set(demos.DEMOS) - {"consumer"}))
    def test_is_a_broker_and_its_clients(self, name):
        roles = [step.role for step in demos.DEMOS[name].processes]
        assert "broker" in roles
        # Started before anything that connects to it.
        assert roles.index("broker") < roles.index("producer")

    def test_waits_on_distinct_ports(self):
        for demo in demos.DEMOS.values():
            ports = [step.port for step in demo.processes if step.port is not None]
            assert len(ports) == len(set(ports))


class TestTheRunner:
    @pytest.mark.parametrize("argv", [[], ["-h"], ["--help"]])
    def test_no_name_lists_every_demo(self, argv, capsys):
        demos.main(argv)
        listed = capsys.readouterr().out
        assert all(f"  {name} " in listed for name in demos.DEMOS)

    @pytest.mark.parametrize("argv", [["nope"], ["--list"], ["--label", "b"]])
    def test_anything_but_a_name_is_refused_with_the_list(self, argv, capsys):
        with pytest.raises(SystemExit) as refused:
            demos.main(argv)

        assert refused.value.code == 2
        assert "no demo called" in capsys.readouterr().err

    def test_help_after_a_name_is_its_subscribers_options(self, capfd):
        demos.main(["trades", "--help"])
        out = capfd.readouterr().out
        assert "ARGS go to its consumer" in out
        assert "--label" in out

    def test_a_subscriber_without_options_says_so(self, capsys):
        demos.main(["book", "--help"])
        assert "its page takes no arguments" in capsys.readouterr().out

    def test_arguments_go_to_the_subscriber(self, monkeypatch):
        ran: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            demos, "run", lambda demo, args: ran.append((demo.about, args))
        )
        demos.main(["trades", "--label", "b"])
        assert ran == [(demos.DEMOS["trades"].about, ["--label", "b"])]

    def test_a_port_already_taken_stops_it_before_it_starts(self, capsys):
        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            port = holder.getsockname()[1]
            demo = demos.Demo(
                "x", [demos.Process("broker", [sys.executable, "-c", "pass"], port)]
            )
            demos.run(demo, [])

        assert f"port {port} is already in use" in capsys.readouterr().err


class TestTheBroker:
    def test_a_schema_is_a_file_or_a_python_attribute(self):
        trades = broker.schema("examples/trades/schema.json")
        assert "trade_id" in trades["properties"]
        assert broker.schema("examples.book.producer:EVENTS") == book.EVENTS

    async def test_a_producer_publishes_through_it_to_a_subscriber(self, tmp_path):
        stream = streamcast.Stream.new(
            "orders", root=tmp_path, schema=broker.schema("examples/book/schema.json")
        )
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=False) as server:
            uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/orders"
            async with streamcast.publish(uri) as publication:
                await publication.send(
                    book.row("order_deleted", {"id": 7, "microtimestamp": "1"})
                )

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                offset, ts, row = await asyncio.wait_for(sub.recv(), 5)

        assert (offset, row["order_id"], row["deleted"]) == (1, 7, True)
        assert isinstance(ts, int)


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

    def test_the_page_subscribes_to_the_broker_it_is_given(self):
        page = open("examples/book/index.html").read()  # noqa: PTH123, SIM115
        assert 'get("broker") || "ws://127.0.0.1:8767"' in page
        assert "${broker}/orders?offset=" in page
