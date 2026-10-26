"""The portable declaration contract, before it is wired into either opener."""

import json
from datetime import timedelta
from typing import get_args

import pyarrow as pa
import pytest
from litelink import LogConfig, S3Options
from pydantic import TypeAdapter, ValidationError

from streamcast import Storage, StreamSpec, to_arrow
from streamcast._limits import MAX_BACKLOG, MAX_IN_FLIGHT, MAX_INBOUND, MAX_PUBLISHERS
from streamcast._spec import Initialization
from streamcast._stream import MAX_REPLAY


@pytest.fixture
def shape():
    return {
        "type": "object",
        "properties": {"ts": {"type": "integer"}, "price": {"type": "number"}},
        "required": ["ts", "price"],
    }


def test_spec_defaults_and_cached_arrow(shape):
    spec = StreamSpec(name="trades", schema=shape)
    assert spec.model_dump() == {
        "name": "trades",
        "schema": shape,
        "sort_by": (),
        "exactly_once": False,
        "replay_published": False,
        "group_commit": True,
        "max_replay": MAX_REPLAY,
        "max_publishers": MAX_PUBLISHERS,
        "max_backlog": MAX_BACKLOG,
        "max_inbound": MAX_INBOUND,
        "max_in_flight": MAX_IN_FLIGHT,
        "max_message_bytes": None,
    }
    assert spec.arrow == to_arrow(shape)
    assert spec.arrow is spec.arrow
    assert "arrow" not in json.loads(spec.model_dump_json())


def test_spec_aliases_roundtrip_and_key_order(shape):
    spec = StreamSpec(name="trades", schema_=shape, sort_by=["ts"])
    assert spec.schema_ == shape
    assert spec.sort_by == ("ts",)
    assert StreamSpec.model_validate(json.loads(spec.model_dump_json())) == spec
    assert StreamSpec.model_validate_json(spec.model_dump_json()) == spec
    reordered = {**shape, "properties": dict(reversed(shape["properties"].items()))}
    assert StreamSpec(name="trades", schema=reordered, sort_by=("ts",)) == spec


@pytest.mark.parametrize("name", ["a", "A0_.-", "a" * 128])
def test_valid_names(name, shape):
    assert StreamSpec(name=name, schema=shape).name == name


@pytest.mark.parametrize("name", ["", "-a", "a/b", "a\n", "a" * 129, "é", 12])
def test_invalid_names(name, shape):
    with pytest.raises(ValidationError):
        StreamSpec(name=name, schema=shape)


@pytest.mark.parametrize("flag", ["exactly_once", "replay_published", "group_commit"])
@pytest.mark.parametrize("value", [True, False])
def test_flags_accept_bools(flag, value, shape):
    assert (
        getattr(StreamSpec(name="trades", schema=shape, **{flag: value}), flag) is value
    )


@pytest.mark.parametrize("flag", ["exactly_once", "replay_published", "group_commit"])
@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_flags_refuse_coercion(flag, value, shape):
    with pytest.raises(ValidationError):
        StreamSpec(name="trades", schema=shape, **{flag: value})


@pytest.mark.parametrize(
    "field",
    [
        "max_publishers",
        "max_backlog",
        "max_inbound",
        "max_in_flight",
        "max_message_bytes",
    ],
)
@pytest.mark.parametrize("value", [0, -1, True, "3", 1.0])
def test_positive_limits(field, value, shape):
    with pytest.raises(ValidationError):
        StreamSpec(name="trades", schema=shape, **{field: value})


@pytest.mark.parametrize(
    "field",
    [
        "max_publishers",
        "max_backlog",
        "max_inbound",
        "max_in_flight",
        "max_message_bytes",
    ],
)
def test_positive_limit_boundary(field, shape):
    assert (
        getattr(
            StreamSpec.model_validate({"name": "trades", "schema": shape, field: 1}),
            field,
        )
        == 1
    )
    if field != "max_message_bytes":
        with pytest.raises(ValidationError):
            StreamSpec.model_validate({"name": "trades", "schema": shape, field: None})


@pytest.mark.parametrize("value", [-1, True, "3", 1.0])
def test_replay_refuses_invalid_bounds(value, shape):
    with pytest.raises(ValidationError):
        StreamSpec(name="trades", schema=shape, max_replay=value)


@pytest.mark.parametrize("value", [0, 1, None])
def test_replay_accepts_zero_and_unlimited(value, shape):
    assert StreamSpec(name="trades", schema=shape, max_replay=value).max_replay == value


@pytest.mark.parametrize("sort", [["ts", "ts"], ["missing"], [1], "ts"])
def test_sort_refuses_duplicates_unknowns_and_non_names(sort, shape):
    with pytest.raises(ValidationError):
        StreamSpec(name="trades", schema=shape, sort_by=sort)


@pytest.mark.parametrize("reserved", ["streamcast_ts", "litelink_offset"])
def test_reserved_columns(reserved, shape):
    shape["properties"][reserved] = {"type": ["integer", "null"]}
    with pytest.raises(ValidationError):
        StreamSpec(name="trades", schema=shape)


@pytest.mark.parametrize(
    "bad",
    [
        {},
        {"type": "array", "properties": {"ts": {"type": "integer"}}},
        {"properties": {"ts": {"type": "integer"}}},
        {"properties": {"ts": {"type": "invalid"}}, "required": ["ts"]},
        {"properties": {"ts": {"type": "integer"}}, "required": ["missing"]},
        {"properties": {"ts": []}},
        {"properties": []},
        {"properties": {"ts": {"type": "integer"}}, "additionalProperties": True},
    ],
)
def test_schema_compilation_failures_are_validation_errors(bad):
    with pytest.raises(ValidationError):
        StreamSpec(name="trades", schema=bad)


@pytest.mark.parametrize(
    "column",
    [
        {"type": "integer", "minimum": 0},
        {"type": "array", "items": {"type": "string", "enum": ["x"]}},
        {"type": "object", "properties": {"v": {"type": "integer", "maximum": 5}}},
        {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
    ],
)
def test_schema_whitelist_at_every_depth(column):
    with pytest.raises(ValidationError):
        StreamSpec(
            name="trades", schema={"properties": {"ts": column}, "required": ["ts"]}
        )


def test_supported_nested_schema():
    shape = {
        "type": "object",
        "properties": {
            "ts": {"type": "integer", "format": "int32"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "counts": {"type": "object", "additionalProperties": {"type": "integer"}},
            "data": {"type": "string", "contentEncoding": "base64"},
        },
        "required": ["ts", "tags", "counts", "data"],
    }
    spec = StreamSpec(name="trades", schema=shape)
    assert spec.arrow.field("ts").type == pa.int32()
    assert spec.arrow == to_arrow(shape)


@pytest.mark.parametrize("field", ["storage_profile", "limits", "initialization"])
def test_unknown_spec_fields_fail_closed(field, shape):
    with pytest.raises(ValidationError):
        StreamSpec.model_validate({"name": "trades", "schema": shape, field: {}})


def test_models_frozen_and_required(shape, tmp_path):
    for model, field, value in [
        (StreamSpec(name="trades", schema=shape), "name", "quotes"),
        (Storage(root=tmp_path), "root", tmp_path / "other"),
    ]:
        with pytest.raises(ValidationError, match="frozen_instance"):
            setattr(model, field, value)

    for data in [{"name": "trades"}, {"schema": shape}]:
        with pytest.raises(ValidationError):
            StreamSpec.model_validate(data)

    with pytest.raises(ValidationError):
        Storage.model_validate({})

    with pytest.raises(ValidationError):
        Storage.model_validate({"root": tmp_path, "credentials": "archive"})


def test_storage_defaults_absolute_root_and_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    storage = Storage(root="~/logs")
    assert storage.root == tmp_path / "logs"
    assert not storage.root.exists()
    assert storage.model_dump() == {
        "root": tmp_path / "logs",
        "published": None,
        "replicate": False,
        "config": None,
    }
    assert Storage.model_validate(json.loads(storage.model_dump_json())) == storage
    for root in ["logs", ".", ""]:
        with pytest.raises(ValidationError):
            Storage(root=root)


@pytest.mark.parametrize("replicate", [True, False])
def test_storage_replication_rules(replicate, tmp_path):
    storage = Storage(
        root=tmp_path, published="s3://example-bucket/streams", replicate=replicate
    )
    assert storage.replicate is replicate
    if replicate:
        for published in [None, ""]:
            with pytest.raises(ValidationError):
                Storage(root=tmp_path, replicate=True, published=published)

    else:
        with pytest.raises(ValidationError):
            Storage(root=tmp_path, config=LogConfig(wal_replication=True))


@pytest.mark.parametrize("value", [1, "true", None])
def test_storage_flag_strict(value, tmp_path):
    with pytest.raises(ValidationError):
        Storage(root=tmp_path, published="archive", replicate=value)


def test_storage_config_roundtrip(tmp_path):
    config = LogConfig(wal_replication=True, staging_retention=timedelta(hours=2))
    storage = Storage(root=tmp_path, published="archive", replicate=True, config=config)
    encoded = storage.model_dump_json()
    assert json.loads(encoded)["config"]["staging_retention"] == "PT2H"
    assert Storage.model_validate(json.loads(encoded)) == storage
    assert Storage.model_validate_json(encoded) == storage
    assert storage.config == config


def test_credentials_never_serialized_printed_or_compared(tmp_path):
    first = Storage(
        root=tmp_path,
        s3_options=S3Options(access_key="test-key-one", secret_key="test-secret-one"),
    )
    second = Storage(
        root=tmp_path,
        s3_options=S3Options(access_key="test-key-two", secret_key="test-secret-two"),
    )
    plain = Storage(root=tmp_path)
    assert first.s3_options is not None
    assert first.s3_options.access_key == "test-key-one"
    assert first == second == plain
    assert hash(first) == hash(second) == hash(plain)
    assert first != Storage(root=tmp_path / "different")
    assert first != Storage(root=tmp_path, published="archive")
    assert first != Storage(root=tmp_path, config=LogConfig(compression="lz4"))
    assert first != object()
    assert "s3_options" not in first.model_dump()
    assert "s3_options" not in first.model_dump_json()
    assert "s3_options" not in repr(first)
    assert "test-key-one" not in repr(first)
    assert "test-secret-one" not in str(first)
    assert Storage.model_validate_json(first.model_dump_json()) == first


def test_initialization_contract():
    choices = get_args(Initialization)
    assert choices == ("open_existing", "create_if_absent", "restore_published")
    adapter = TypeAdapter(Initialization)
    for choice in choices:
        assert adapter.validate_python(choice) == choice

    with pytest.raises(ValidationError):
        adapter.validate_python("create")
