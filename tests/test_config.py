import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Event

import pytest
from tix_config import (
    MISSING,
    CallableSource,
    ConfigManager,
    EnvSource,
    FileSource,
    MappingSource,
    Secret,
    SecretDirectorySource,
    Snapshot,
    SourceError,
    deep_merge,
    resolve_references,
    save_json,
)


def test_layering_provenance_and_immutable_versions(tmp_path):
    path = tmp_path / "settings.toml"
    path.write_text('[db]\nhost = "localhost"\nport = 5432\n', encoding="utf-8")
    manager = ConfigManager(
        [
            MappingSource({"db": {"port": 1234, "pool": 8}}, "defaults"),
            FileSource(path),
            EnvSource(environ={"APP_DB__PORT": "9000", "IGNORED": "true"}),
        ]
    )
    change = manager.reload()
    original = manager.snapshot
    assert original.at("db") == {"host": "localhost", "port": 9000, "pool": 8}
    assert original.origin("db.port") == "environment"
    assert original.origin("db.pool") == "defaults"
    assert original.origin("db.host") == str(path)
    assert change.changed and original.version == 1
    with pytest.raises(TypeError):
        original["db"]["port"] = 5
    assert not manager.reload().changed
    assert manager.snapshot is original
    path.write_text('[db]\nhost = "new"\n', encoding="utf-8")
    manager.reload()
    assert manager.snapshot.version == 2
    assert original.at("db.host") == "localhost"


def test_validation_and_bad_source_leave_previous_snapshot():
    data = {"port": 1}

    def validate(snapshot):
        if snapshot["port"] < 1:
            raise ValueError("invalid port")

    manager = ConfigManager([MappingSource(data)], validators=[validate])
    manager.reload()
    previous = manager.snapshot
    data["port"] = -1
    with pytest.raises(ValueError, match="invalid port"):
        manager.reload()
    assert manager.snapshot is previous
    data["port"] = object()
    with pytest.raises(SourceError):
        manager.reload()
    assert manager.snapshot is previous


def test_merge_policies_overlays_and_decoder():
    manager = ConfigManager([MappingSource({"a": [1], "b": {"x": 1}})], lists="append")
    manager.reload()
    overlay = manager.overlay({"a": [2], "b": {"y": 2}})
    assert overlay["a"] == (1, 2)
    assert manager.snapshot["a"] == (1,)
    assert overlay.decode(lambda data: data["y"], "b") == 2
    assert overlay.origin("b.y") == "overlay"
    assert deep_merge({"x": [1]}, {"x": [2]}) == {"x": [2]}
    assert deep_merge({"x": {"a": 1}}, {"x": 0}) == {"x": 0}
    assert deep_merge({"x": 0}, {"x": {"a": 1}}) == {"x": {"a": 1}}


def test_snapshot_paths_diff_export_and_supported_types():
    snapshot = Snapshot({"a.b": 1, "items": [{"token": Secret("private")}], "date": date.today()})
    assert snapshot.at(("a.b",)) == 1
    assert snapshot.at("missing", None) is None
    with pytest.raises(KeyError):
        snapshot.at("missing")
    assert "private" not in repr(snapshot)
    assert "private" not in repr(Secret("private"))
    exported = snapshot.to_dict()
    assert exported["items"] == [{"token": "********"}]
    assert snapshot.to_dict(reveal_secrets=True)["items"][0]["token"] == "private"
    assert (
        snapshot.to_dict(redact=["items.*.token"], reveal_secrets=True)["items"][0]["token"]
        == "********"
    )
    (change,) = Snapshot({}).diff(Snapshot({"new": None}))
    assert change.before is MISSING and change.after is None
    (change,) = Snapshot({"new": None}).diff(Snapshot({}))
    assert change.after is MISSING


def test_subscribers_reentrant_reload_and_unsubscribe():
    data = {"n": 1}
    manager = ConfigManager([MappingSource(data)])
    observed = []
    unsubscribe = manager.subscribe(lambda result: observed.append(result.current.version))
    manager.subscribe(lambda result: manager.reload())
    with pytest.raises(ExceptionGroup, match="published") as failure:
        manager.reload()
    assert isinstance(failure.value.exceptions[0], RuntimeError)
    assert manager.snapshot["n"] == 1
    assert observed == [1]
    unsubscribe()
    unsubscribe()
    data["n"] = 2
    with pytest.raises(ExceptionGroup):
        manager.reload()
    assert observed == [1]


def test_reload_does_not_block_snapshot_reads():
    entered, release = Event(), Event()

    def load():
        entered.set()
        assert release.wait(5)
        return {"value": 2}

    manager = ConfigManager([CallableSource(load)])
    with ThreadPoolExecutor() as executor:
        future = executor.submit(manager.reload)
        assert entered.wait(5)
        try:
            assert manager.snapshot.version == 0
        finally:
            release.set()
        assert future.result(5).current["value"] == 2


def test_json_toml_yaml_optional_file_and_custom_parser(tmp_path):
    for extension, text in [("json", '{"x": 1}'), ("toml", "x = 1"), ("yaml", "x: 1")]:
        path = tmp_path / f"data.{extension}"
        path.write_text(text, encoding="utf-8")
        assert FileSource(path).load() == {"x": 1}
    assert FileSource(tmp_path / "absent.json", required=False).load() == {}
    with pytest.raises(FileNotFoundError):
        FileSource(tmp_path / "absent.json").load()
    path = tmp_path / "data.custom"
    path.write_text("1", encoding="utf-8")
    assert FileSource(path, parser=lambda text: {"x": int(text)}).load() == {"x": 1}
    with pytest.raises(ValueError):
        FileSource(path).load()
    path = tmp_path / "array.json"
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(TypeError):
        FileSource(path).load()


def test_environment_conflicts_and_parsing():
    source = EnvSource(environ={"APP_INT": "3", "APP_BOOL": "true", "APP_TEXT": "yes"})
    assert source.load() == {"int": 3, "bool": True, "text": "yes"}
    assert EnvSource(environ={}).load() == {}
    for values in [{"APP_A": "1", "APP_A__B": "2"}, {"APP_A": "1", "APP_a": "2"}]:
        with pytest.raises(ValueError, match="conflicting"):
            EnvSource(environ=values).load()
    with pytest.raises(ValueError):
        EnvSource(delimiter="")


def test_secret_source_allowlist(tmp_path):
    (tmp_path / "password").write_text(" secret \n", encoding="utf-8")
    source = SecretDirectorySource(tmp_path, ("password", "absent"), required=False)
    assert source.load() == {"password": Secret(" secret ")}
    for name in ("../password", "..", "C:password", "folder\\password"):
        with pytest.raises(ValueError):
            SecretDirectorySource(tmp_path, (name,)).load()


def test_interpolation_types_nested_sequences_cycles_and_secrets():
    data = {
        "port": 42,
        "url": "http://localhost:${port}",
        "copy": "${port}",
        "nested": {"a": "${port}"},
        "rows": [{"v": "${nested.a}"}],
    }
    result = resolve_references(data)
    assert result["copy"] == 42
    assert result["url"] == "http://localhost:42"
    assert result["rows"] == [{"v": 42}]
    with pytest.raises(ValueError, match="cyclic"):
        resolve_references({"a": "${b}", "b": "${a}"})
    with pytest.raises(ValueError, match="cyclic"):
        resolve_references({"a": {"x": "${a}"}})
    with pytest.raises(KeyError):
        resolve_references({"a": "${missing}"})
    with pytest.raises(TypeError):
        resolve_references({"secret": Secret("hidden"), "copy": "oops:${secret}"})
    manager = ConfigManager([MappingSource({"a": {"x": 1}, "b": "${a}"})], interpolate=True)
    manager.reload()
    assert manager.snapshot.at("b.x") == 1
    assert manager.snapshot.origin("b.x") == "mapping"


def test_atomic_save_preserves_original_on_serialization_failure(tmp_path):
    path = tmp_path / "settings.json"
    save_json(path, {"value": [1]})
    assert json.loads(path.read_text()) == {"value": [1]}
    with pytest.raises(TypeError):
        save_json(path, {"password": Secret("private")})
    assert json.loads(path.read_text()) == {"value": [1]}
    assert list(tmp_path.iterdir()) == [path]


def test_custom_merge_and_invalid_source_names():
    manager = ConfigManager(
        [MappingSource({"a": 1}, "first"), MappingSource({"b": 2}, "second")],
        merge=lambda previous, incoming: incoming,
    )
    manager.reload()
    assert manager.snapshot.to_dict() == {"b": 2}
    with pytest.raises(ValueError, match="unique"):
        ConfigManager([MappingSource({}), MappingSource({})])
    with pytest.raises(TypeError):
        Snapshot({1: "bad"})
