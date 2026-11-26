"""Exercise release shell steps offline; no image or chart is published."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).parents[1] / ".github/workflows/release.yml"
IMAGE = "ghcr.io/example/streamcast"
SHA = "c" * 40


def _workflow():
    # BaseLoader keeps Actions' `on` key a string rather than a YAML 1.1 bool.
    return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)


def _step(job, name):
    return next(s for s in _workflow()["jobs"][job]["steps"] if s.get("name") == name)


def _run(tmp_path, job, name, responses, tag="v0.30.0"):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls.jsonl"
    calls.touch()
    for tool, answers in responses.items():
        executable = bin_dir / tool
        executable.write_text(
            f"#!{sys.executable}\n"
            "import json\nimport sys\nfrom pathlib import Path\n\n"
            f"with Path({str(calls)!r}).open('a') as trace:\n"
            "    trace.write(json.dumps(sys.argv) + '\\n')\n\n"
            f"for prefix, output, code in {answers!r}:\n"
            "    if sys.argv[1:1 + len(prefix)] == prefix:\n"
            "        print(output, end='')\n"
            "        sys.exit(code)\n\n"
            "raise SystemExit('unexpected tool invocation')\n"
        )
        executable.chmod(0o755)

    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _step(job, name)["run"]],
        cwd=tmp_path,
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "GITHUB_REF_NAME": tag,
            "GITHUB_SHA": SHA,
            "IMAGE": IMAGE,
            "CHART_REGISTRY": "oci://ghcr.io/example/charts",
            "GHCR_USER": "test-publisher",
            "GHCR_TOKEN": "test-only-token",
        },
        capture_output=True,
        text=True,
        timeout=5,
    )
    trace = [json.loads(line) for line in calls.read_text().splitlines()]
    return result, [(Path(args[0]).name, args[1:]) for args in trace]


def test_publication_keeps_the_tag_and_approval_gates():
    workflow = _workflow()
    assert workflow["on"] == {"push": {"tags": ["v*"]}}
    assert workflow["permissions"] == {"contents": "read"}
    jobs = workflow["jobs"]
    assert jobs["publish"]["environment"]["name"] == "pypi"
    assert jobs["ghcr-build"]["needs"] == "publish"
    assert jobs["ghcr-image"]["needs"] == "ghcr-build"
    assert jobs["ghcr-chart"]["needs"] == "publish"
    assert jobs["ghcr-latest"]["needs"] == ["ghcr-image", "github-release"]
    assert jobs["ghcr-latest"]["concurrency"] == {
        "group": "ghcr-latest",
        "cancel-in-progress": "false",
    }
    for name in ("ghcr-build", "ghcr-image", "ghcr-chart", "ghcr-latest"):
        assert jobs[name]["permissions"]["packages"] == "write"
        assert "if" not in jobs[name]  # A failed dependency cannot be bypassed.
        if name != "ghcr-latest":
            assert "concurrency" not in jobs[name]


def test_native_architectures_publish_by_digest():
    job = _workflow()["jobs"]["ghcr-build"]
    assert job["strategy"]["matrix"]["include"] == [
        {"arch": "amd64", "runner": "ubuntu-24.04"},
        {"arch": "arm64", "runner": "ubuntu-24.04-arm"},
    ]
    build = next(s for s in job["steps"] if s.get("id") == "build")
    assert build["with"]["platforms"] == "linux/${{ matrix.arch }}"
    assert build["with"]["file"] == "docker/Dockerfile"
    assert build["with"]["outputs"] == (
        "type=image,name=${{ env.IMAGE }},"
        "push-by-digest=true,name-canonical=true,push=true"
    )
    upload = next(s for s in job["steps"] if "upload-artifact" in s.get("uses", ""))
    assert upload["with"]["name"] == "image-digest-${{ matrix.arch }}"
    assert upload["with"]["retention-days"] == "1"
    download = _workflow()["jobs"]["ghcr-image"]["steps"][0]
    assert download["with"] == {
        "pattern": "image-digest-*",
        "path": "digests",
        "merge-multiple": "true",
    }


@pytest.mark.parametrize(
    "hashes", [("a" * 64, "b" * 64), ("a" * 64,), ("bad", "b" * 64)]
)
def test_merge_requires_two_digests_and_tags_the_same_manifest(tmp_path, hashes):
    (tmp_path / "digests").mkdir()
    for digest in hashes:
        (tmp_path / "digests" / digest).touch()

    # Create a trace even when validation refuses before docker is called.
    (tmp_path / "calls.jsonl").touch()
    result, calls = _run(
        tmp_path,
        "ghcr-image",
        "Merge release digests",
        {"docker": [(["buildx", "imagetools", "create"], "", 0)]},
    )
    if hashes != ("a" * 64, "b" * 64):
        assert result.returncode != 0
        assert not calls
        return

    assert result.returncode == 0, result.stderr
    assert calls == [
        (
            "docker",
            [
                "buildx",
                "imagetools",
                "create",
                "--tag",
                f"{IMAGE}:0.30.0",
                "--tag",
                f"{IMAGE}:{SHA}",
                f"{IMAGE}@sha256:{hashes[0]}",
                f"{IMAGE}@sha256:{hashes[1]}",
            ],
        )
    ]


@pytest.mark.parametrize(
    ("tag", "tags", "promote"),
    [
        ("v0.30.0", "v0.9.0\nv0.30.0\nv0.30.1rc1\n", True),
        ("v0.9.0", "v0.9.0\nv0.30.0\n", False),
        ("v0.30.1rc1", "v0.30.0\nv0.30.1rc1\n", False),
        ("v0.30.1rc1", "v0.30.1rc1\n", False),
    ],
)
def test_latest_never_moves_backwards_or_to_a_prerelease(tmp_path, tag, tags, promote):
    result, calls = _run(
        tmp_path,
        "ghcr-latest",
        "Promote latest only for the newest stable tag",
        {
            "git": [
                (["fetch", "--force", "--tags", "origin"], "", 0),
                (["tag", "--list", "v*"], tags, 0),
            ],
            "docker": [(["buildx", "imagetools", "create"], "", 0)],
        },
        tag,
    )
    assert result.returncode == 0, result.stderr
    assert calls[0] == ("git", ["fetch", "--force", "--tags", "origin"])
    pushes = [args for tool, args in calls if tool == "docker"]
    assert pushes == (
        [
            [
                "buildx",
                "imagetools",
                "create",
                "--tag",
                f"{IMAGE}:latest",
                f"{IMAGE}:{tag[1:]}",
            ]
        ]
        if promote
        else []
    )


@pytest.mark.parametrize(
    ("version", "app_version", "lint_code", "success"),
    [
        ("0.30.0", "0.30.0", 0, True),
        ("0.29.1", "0.30.0", 0, False),
        ("0.30.0", "0.29.1", 0, False),
        ("0.30.0", "0.30.0", 1, False),
    ],
)
def test_chart_requires_matching_versions_and_lint(
    tmp_path, version, app_version, lint_code, success
):
    metadata = f'version: {version}\nappVersion: "{app_version}"\n'
    result, calls = _run(
        tmp_path,
        "ghcr-chart",
        "Package the matching release chart",
        {
            "helm": [
                (["show", "chart", "charts/streamcast"], metadata, 0),
                (["lint", "charts/streamcast", "--strict"], "", lint_code),
                (
                    ["package", "charts/streamcast", "--destination", "chart-dist"],
                    "",
                    0,
                ),
            ]
        },
    )
    assert (result.returncode == 0) == success, result.stderr
    packages = [args for _, args in calls if args[0] == "package"]
    assert packages == (
        [["package", "charts/streamcast", "--destination", "chart-dist"]]
        if success
        else []
    )


def test_chart_push_uses_versioned_archive_and_password_stdin(tmp_path):
    result, calls = _run(
        tmp_path,
        "ghcr-chart",
        "Publish the chart",
        {"helm": [(["registry", "login"], "", 0), (["push"], "", 0)]},
    )
    assert result.returncode == 0, result.stderr
    assert calls == [
        (
            "helm",
            [
                "registry",
                "login",
                "ghcr.io",
                "--username",
                "test-publisher",
                "--password-stdin",
            ],
        ),
        (
            "helm",
            [
                "push",
                "chart-dist/streamcast-0.30.0.tgz",
                "oci://ghcr.io/example/charts",
            ],
        ),
    ]
