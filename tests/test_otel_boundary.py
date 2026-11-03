"""Libraries use the OTel API; applications own the SDK and exporters."""

from __future__ import annotations

import ast
import importlib.metadata
import subprocess
import sys
from pathlib import Path

from packaging.requirements import Requirement

SOURCE = Path(__file__).parents[1] / "src" / "streamcast"
FORBIDDEN = (
    "opentelemetry.sdk",
    "opentelemetry.exporter",
    "opentelemetry.instrumentation",
)


def forbidden_imports(source: str) -> list[tuple[int, str]]:
    found = []
    for node in ast.walk(ast.parse(source)):
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]

        elif isinstance(node, ast.ImportFrom):
            names = [f"{node.module}.{alias.name}" for alias in node.names]

        elif isinstance(node, ast.Call) and node.args:
            func = node.func
            if (isinstance(func, ast.Attribute) and func.attr == "import_module") or (
                isinstance(func, ast.Name) and func.id == "import_module"
            ):
                arg = node.args[0]
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    names = [arg.value]

        for name in names:
            if any(
                name == prefix or name.startswith(prefix + ".") for prefix in FORBIDDEN
            ):
                found.append((getattr(node, "lineno", 0), name))

    return found


def test_source_imports_only_the_api():
    violations = [
        f"{path.relative_to(SOURCE.parent)}:{line}: {name}"
        for path in sorted(SOURCE.rglob("*.py"))
        for line, name in forbidden_imports(path.read_text())
    ]
    assert not violations, "\n".join(violations)


def test_fresh_interpreter_imports_no_sdk():
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import importlib, pkgutil, sys, streamcast\n"
            "for module in pkgutil.walk_packages(streamcast.__path__, 'streamcast.'):\n"
            "    importlib.import_module(module.name)\n"
            "loaded = [name for name in sys.modules "
            "if name == 'opentelemetry.sdk' or name.startswith('opentelemetry.sdk.')]\n"
            "assert not loaded, loaded\n",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_runtime_metadata_requires_api_without_sdk():
    requirements = [
        Requirement(value) for value in importlib.metadata.requires("streamcast") or []
    ]
    names = {req.name for req in requirements}
    assert "opentelemetry-api" in names
    assert "opentelemetry-sdk" not in names
    api = next(req for req in requirements if req.name == "opentelemetry-api")
    assert api.marker is None
    assert "1.30.0" in api.specifier
    assert "1.29.0" not in api.specifier
    assert "2.0.0" not in api.specifier
