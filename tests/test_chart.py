"""Render manifests; kind exercises external scale ownership and OOM isolation."""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import threading
import time
import tomllib
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml

from streamcast import _config_file
from streamcast._broker.model import ExternalMaintain

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts" / "streamcast"
HELM = shutil.which("helm") or "helm"
STOP_ORDER = ["clean-published", "clean", "compact", "publish", "seal"]
pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is required for chart tests"
)


class ChartAccess:
    async def authenticate(self, scope):
        return "operator"

    async def allows(self, principal, action, resource):
        return True


AUTH = ChartAccess()


def render(tmp_path, values=None, *, check=True):
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values or {}))
    result = subprocess.run(
        [HELM, "template", "test", str(CHART), "--values", str(path)],
        text=True,
        capture_output=True,
        check=check,
    )
    if not check:
        return result

    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def statefulset(objects):
    return next(obj for obj in objects if obj["kind"] == "StatefulSet")


def pod(objects):
    return statefulset(objects)["spec"]["template"]["spec"]


def config(objects):
    text = next(obj for obj in objects if obj["kind"] == "ConfigMap")["data"][
        "broker.toml"
    ]
    return tomllib.loads(text)


def test_chart_versions_and_kubernetes_floor():
    chart = yaml.safe_load((CHART / "Chart.yaml").read_text())
    package = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert chart["version"] == chart["appVersion"] == package["project"]["version"]
    result = subprocess.run(
        [HELM, "template", "test", str(CHART), "--kube-version", "1.28.0"],
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0
    assert "kubeVersion" in result.stderr


def test_kind_image_loader_supports_containerd_v2():
    workflow = yaml.safe_load((ROOT / ".github/workflows/chart.yml").read_text())
    cluster = next(
        step
        for step in workflow["jobs"]["kind"]["steps"]
        if step.get("uses", "").startswith("helm/kind-action@")
    )
    # The node image uses containerd 2. An older kind creates the cluster,
    # but cannot detect its snapshotter when loading the locally built image.
    version = cluster["with"].get("version")
    assert version is not None, "pin kind instead of using the action's old default"
    assert tuple(map(int, version.removeprefix("v").split("."))) >= (0, 27, 0)


def test_kind_requires_single_process_oom_patch():
    workflow = yaml.safe_load((ROOT / ".github/workflows/chart.yml").read_text())
    steps = workflow["jobs"]["kind"]["steps"]
    action = next(
        step for step in steps if step.get("uses", "").startswith("helm/kind-action@")
    )
    assert action["with"]["node_image"] == "kindest/node:v1.32.2"
    assert action["with"]["config"] == "kind-config.yaml"
    writer = next(
        step["run"]
        for step in steps
        if step.get("name") == "Configure single-process OOM killing"
    )
    document = yaml.safe_load(writer.split("<<'EOF'\n")[1].rsplit("EOF", 1)[0])
    patches = [yaml.safe_load(patch) for patch in document["kubeadmConfigPatches"]]
    assert any(
        patch.get("kind") == "KubeletConfiguration"
        and patch.get("singleProcessOOMKill") is True
        for patch in patches
    )


def test_roles_are_native_sidecars_in_reverse_stop_order(tmp_path):
    objects = render(
        tmp_path,
        {
            "maintenance": {"groups": [{"name": "default"}, {"name": "archive"}]},
            "broker": {
                "config": {"storage_profiles": {"archive": {"root": "/archive"}}}
            },
        },
    )
    spec = pod(objects)
    roles = spec["initContainers"]
    assert [item["name"] for item in roles] == [
        f"{role}-{group}" for role in STOP_ORDER for group in ("default", "archive")
    ]
    assert all(item["restartPolicy"] == "Always" for item in roles)
    assert [item["name"] for item in spec["containers"]] == ["broker"]
    assert spec["restartPolicy"] == "Always"
    workload = statefulset(objects)["spec"]
    assert workload["replicas"] == 1
    assert workload["podManagementPolicy"] == "OrderedReady"
    assert workload["updateStrategy"] == {"type": "RollingUpdate"}
    for item in roles:
        role, group = item["args"][1], item["args"][3]
        command = item["command"]
        status = f"/run/streamcast/{role}-{group}.supervise.json"
        assert command[:5] == ["tini", "--", "streamcast-supervise", "--status", status]
        assert command[5:18] == [
            "--run-dir",
            "/run/streamcast",
            "--role",
            role,
            "--group",
            group,
            "--probe-period",
            "10",
            "--probe-threshold",
            "6",
            "--probe-timeout",
            "15",
            "--",
        ]
        assert command[18:] == ["streamcast", "maintain", "--follow", "/run/streamcast"]
        assert item["livenessProbe"]["exec"]["command"] == [
            "streamcast-supervise",
            "probe",
            "--status",
            status,
            "--max-age",
            "75",
        ]


def test_roles_have_no_readiness_or_startup_probe(tmp_path):
    spec = pod(render(tmp_path))
    for role in spec["initContainers"]:
        assert "livenessProbe" in role
        assert "readinessProbe" not in role
        assert "startupProbe" not in role

    broker = spec["containers"][0]
    assert broker["startupProbe"]["httpGet"]["path"] == "/readyz"
    assert broker["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert broker["livenessProbe"]["httpGet"]["path"] == "/livez"


def test_existing_claim_renders_no_claim_template(tmp_path):
    objects = render(tmp_path, {"persistence": {"existingClaim": "adopted"}})
    assert "volumeClaimTemplates" not in statefulset(objects)["spec"]
    assert "persistentVolumeClaimRetentionPolicy" not in statefulset(objects)["spec"]
    volume = next(item for item in pod(objects)["volumes"] if item["name"] == "data")
    assert volume["persistentVolumeClaim"]["claimName"] == "adopted"


def test_new_claim_is_single_pod_and_retained(tmp_path):
    workload = statefulset(render(tmp_path))["spec"]
    assert workload["persistentVolumeClaimRetentionPolicy"] == {
        "whenDeleted": "Retain",
        "whenScaled": "Retain",
    }
    claim = workload["volumeClaimTemplates"][0]
    assert claim["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert claim["spec"]["accessModes"] == ["ReadWriteOncePod"]
    assert claim["spec"]["resources"]["requests"]["storage"] == "20Gi"


@pytest.mark.parametrize("values", [{}, {"scaleOwner": "chart"}])
def test_default_scale_owner_renders_replicas_one(tmp_path, values):
    defaults = yaml.safe_load((CHART / "values.yaml").read_text())
    assert defaults["scaleOwner"] == "chart"
    assert statefulset(render(tmp_path, values))["spec"]["replicas"] == 1


@pytest.mark.parametrize("command", [[], ["custom-broker"]])
def test_external_scale_omits_replicas(tmp_path, command):
    objects = render(
        tmp_path,
        {
            "scaleOwner": "external",
            "persistence": {"existingClaim": "data"},
            "broker": {"command": command},
        },
    )
    workload = statefulset(objects)["spec"]
    assert "replicas" not in workload
    assert "volumeClaimTemplates" not in workload
    assert not any(obj["kind"] == "PersistentVolumeClaim" for obj in objects)
    volume = next(item for item in pod(objects)["volumes"] if item["name"] == "data")
    assert volume["persistentVolumeClaim"]["claimName"] == "data"


@pytest.mark.parametrize("command", [[], ["custom-broker"]])
def test_external_scale_requires_existing_claim(tmp_path, command):
    result = render(
        tmp_path,
        {"scaleOwner": "external", "broker": {"command": command}},
        check=False,
    )
    assert result.returncode != 0
    assert "scaleOwner external requires persistence.existingClaim" in result.stderr


@pytest.mark.parametrize("owner", ["unknown", "", None])
@pytest.mark.parametrize("command", [[], ["custom-broker"]])
def test_unknown_scale_owner_fails(tmp_path, owner, command):
    result = render(
        tmp_path,
        {"scaleOwner": owner, "broker": {"command": command}},
        check=False,
    )
    assert result.returncode != 0
    assert "scaleOwner must be chart or external" in result.stderr


@pytest.mark.parametrize("owner", ["chart", "external"])
@pytest.mark.parametrize("replicas", [0, 2, 3])
def test_replicas_other_than_one_fails(tmp_path, replicas, owner):
    result = render(
        tmp_path,
        {
            "replicas": replicas,
            "scaleOwner": owner,
            "persistence": {"existingClaim": "data"},
        },
        check=False,
    )
    assert result.returncode != 0
    assert "replicas must be 1" in result.stderr


def test_per_role_memory_limits_rendered(tmp_path):
    resources = {
        role: {
            "resources": {
                "requests": {"memory": f"{128 + i}Mi"},
                "limits": {"memory": f"{512 + i}Mi"},
            }
        }
        for i, role in enumerate(STOP_ORDER)
    }
    roles = pod(render(tmp_path, {"maintenance": {"roles": resources}}))[
        "initContainers"
    ]
    for container, role in zip(roles, STOP_ORDER, strict=True):
        assert (
            container["resources"]["requests"]["memory"]
            == resources[role]["resources"]["requests"]["memory"]
        )
        assert (
            container["resources"]["limits"]["memory"]
            == resources[role]["resources"]["limits"]["memory"]
        )


@pytest.mark.parametrize(
    "selector",
    [
        {"karpenter.sh/nodepool": "system"},
        {"karpenter.sh/capacity-type": "on-demand"},
    ],
)
def test_placement_passthrough(tmp_path, selector):
    placement = {
        "nodeSelector": selector,
        "tolerations": [
            {"key": "dedicated", "operator": "Exists", "effect": "NoSchedule"}
        ],
        "affinity": {
            "nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": []}
        },
        "topologySpreadConstraints": [
            {
                "maxSkew": 1,
                "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "ScheduleAnyway",
            }
        ],
    }
    spec = pod(
        render(tmp_path, {"placement": placement, "priorityClassName": "broker"})
    )
    assert {key: spec[key] for key in placement} == placement
    assert spec["priorityClassName"] == "broker"
    if "karpenter.sh/nodepool" in selector:
        assert "karpenter.sh/capacity-type" not in spec["nodeSelector"]


def test_service_disabled_renders_no_service(tmp_path):
    objects = render(tmp_path, {"service": {"enabled": False}})
    assert not any(obj["kind"] == "Service" for obj in objects)


@pytest.mark.parametrize("port_name", ["http", "events", "metrics"])
def test_named_ports_per_role_of_broker(tmp_path, port_name):
    objects = render(tmp_path, {"broker": {"portName": port_name, "port": 9876}})
    broker = pod(objects)["containers"][0]
    assert broker["ports"] == [{"name": port_name, "containerPort": 9876}]
    for probe in ("livenessProbe", "readinessProbe", "startupProbe"):
        assert broker[probe]["httpGet"]["port"] == port_name

    services = [obj for obj in objects if obj["kind"] == "Service"]
    assert len(services) == 2
    for service in services:
        assert service["spec"]["ports"][0]["targetPort"] == port_name


def test_rendered_config_selects_external_maintenance_with_run_dir_and_groups(tmp_path):
    objects = render(
        tmp_path,
        {
            "broker": {
                "authorization": "tests.test_chart:AUTH",
                "config": {
                    "registry": str(tmp_path / "registry.db"),
                    "storage_profiles": {"archive": {"root": str(tmp_path / "data")}},
                    "maintain": {
                        "seal_every": 0.5,
                        "duckdb_default_threads": 1,
                        "duckdb_default_memory_mib": 192,
                    },
                },
            },
            "maintenance": {
                "runDir": {"path": "/run/custom"},
                "groups": [{"name": "default"}, {"name": "archive"}],
            },
        },
    )
    document = config(objects)
    assert document["broker"]["maintain"]["external"] == {
        "run_dir": "/run/custom",
        "groups": ["default", "archive"],
    }
    config_path = tmp_path / "broker.toml"
    config_path.write_text(
        next(obj for obj in objects if obj["kind"] == "ConfigMap")["data"][
            "broker.toml"
        ]
    )
    settings = _config_file.load(config_path)
    assert isinstance(settings.broker.maintain, ExternalMaintain)
    assert settings.broker.maintain.run_dir == Path("/run/custom")
    assert dict(settings.broker.maintain.groups) == {"default": None, "archive": None}
    assert settings.broker.maintain.policy.seal_every == 0.5
    assert settings.broker.maintain.policy.duckdb_default_threads == 1
    assert settings.broker.authorization is AUTH
    for container in pod(objects)["initContainers"]:
        assert container["command"][-1] == "/run/custom"


def test_group_without_storage_profile_fails(tmp_path):
    result = render(
        tmp_path, {"maintenance": {"groups": [{"name": "missing"}]}}, check=False
    )
    assert result.returncode != 0
    assert "every maintenance group must name" in result.stderr


def test_registry_without_authorization_fails(tmp_path):
    result = render(
        tmp_path,
        {"broker": {"config": {"registry": "/var/lib/streamcast/registry.db"}}},
        check=False,
    )
    assert result.returncode != 0
    assert "broker.authorization is required" in result.stderr


def test_static_config_preserves_nested_schema_and_storage(tmp_path):
    stream = {
        "spec": {
            "name": "events",
            "schema": {
                "type": "object",
                "properties": {
                    "value": {"type": "integer"},
                    "label": {"type": ["string", "null"]},
                },
                "required": ["value"],
            },
        },
        "storage_profile": "default",
    }
    objects = render(tmp_path, {"broker": {"config": {"streams": [stream]}}})
    assert config(objects)["streams"] == [stream]
    config_path = tmp_path / "broker.toml"
    config_path.write_text(
        next(obj for obj in objects if obj["kind"] == "ConfigMap")["data"][
            "broker.toml"
        ]
    )
    assert _config_file.load(config_path).streams[0].spec.name == "events"


def test_shared_volumes_security_images_and_group_credentials(tmp_path):
    objects = render(
        tmp_path,
        {
            "broker": {
                "image": {"repository": "custom-broker", "tag": "test"},
                "env": [
                    {"name": "STREAMCAST_CONTROL_TOKEN_FILE", "value": "/control/token"}
                ],
            },
            "maintenance": {
                "groups": [
                    {
                        "name": "default",
                        "env": [{"name": "AWS_REGION", "value": "us-east-1"}],
                        "envFrom": [{"secretRef": {"name": "store-access"}}],
                    }
                ]
            },
            "persistence": {"aliasMounts": ["/legacy/data"]},
            "extraVolumes": [{"name": "ca", "configMap": {"name": "ca"}}],
            "extraVolumeMounts": [
                {"name": "ca", "mountPath": "/etc/custom-ca", "readOnly": True}
            ],
        },
    )
    spec = pod(objects)
    volumes = {item["name"]: item for item in spec["volumes"]}
    assert volumes["run"]["emptyDir"] == {"medium": "Memory", "sizeLimit": "16Mi"}
    assert volumes["tmp"]["emptyDir"] == {"sizeLimit": "8Gi"}
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert spec["automountServiceAccountToken"] is False
    assert spec["terminationGracePeriodSeconds"] == 60
    for container in spec["containers"] + spec["initContainers"]:
        assert container["image"] == "custom-broker:test"
        assert container["securityContext"] == {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        }
        mounts = container["volumeMounts"]
        assert {item["mountPath"] for item in mounts if item["name"] == "data"} == {
            "/var/lib/streamcast",
            "/legacy/data",
        }
        assert {item["mountPath"] for item in mounts} >= {
            "/run/streamcast",
            "/tmp",
            "/etc/custom-ca",
        }

    for role in spec["initContainers"]:
        assert role["env"] == [{"name": "AWS_REGION", "value": "us-east-1"}]
        assert role["envFrom"] == [{"secretRef": {"name": "store-access"}}]
        assert "config" not in {item["name"] for item in role["volumeMounts"]}

    assert not any(obj["kind"] == "Secret" for obj in objects)


def test_custom_command_ignores_config_and_parent_labels_keep_selectors(tmp_path):
    labels = {"app.kubernetes.io/component": "events", "endpoint": "private"}
    objects = render(
        tmp_path,
        {
            "broker": {
                "command": ["python", "-m", "custom_broker"],
                "config": {"registry": "/unused.db"},
            },
            "maintenance": {"groups": [{"name": "ambient"}]},
            "podLabels": labels,
        },
    )
    assert not any(obj["kind"] == "ConfigMap" for obj in objects)
    workload = statefulset(objects)["spec"]
    pod_labels = workload["template"]["metadata"]["labels"]
    assert all(pod_labels[key] == value for key, value in labels.items())
    assert all(
        pod_labels[key] == value
        for key, value in workload["selector"]["matchLabels"].items()
    )
    assert pod(objects)["containers"][0]["command"] == ["python", "-m", "custom_broker"]


def pod_state(document):
    # PodCondition uses type; ContainerStatus uses name, including native sidecars.
    status = document["status"]
    return {
        "uid": document["metadata"]["uid"],
        "pod_ip": status["podIP"],
        "ready": {item["type"]: item for item in status["conditions"]}["Ready"],
        "containers": {
            item["name"]: item
            for item in status["containerStatuses"] + status["initContainerStatuses"]
        },
    }


def assert_single_process_oom(snapshot, expected_roles):
    assert expected_roles and len(expected_roles) % 5 == 0
    assert len(set(expected_roles)) == len(expected_roles)
    assert snapshot["containers"] and set(snapshot["containers"]) == {
        "broker",
        *expected_roles,
    }
    assert set(snapshot["oom_groups"]) == set(expected_roles)
    assert all(value == 0 for value in snapshot["oom_groups"].values())


def assert_role_oom_isolated(
    before, after, expected_roles, samples, signal_at, recovered_at
):
    # K0: count and identity precede every quantified assertion.
    for snapshot in (before, after):
        assert_single_process_oom(snapshot, expected_roles)

    # K1: the role's cgroup records exactly one actual OOM victim.
    assert isinstance(before["oom_kills"], int) and before["oom_kills"] >= 0
    assert after["oom_kills"] == before["oom_kills"] + 1
    # K2: the supervisor replaced its child, recorded the OOM, and still beats.
    initial, final = before["supervisor"], after["supervisor"]
    assert initial["pid"] > 1 and final["pid"] > 1 and initial["pid"] != final["pid"]
    assert initial["restarts"] >= 0 and final["restarts"] == initial["restarts"] + 1
    assert final["last_exit"]["signal"] == 9 and final["last_exit"]["oom"] is True
    assert initial["child_started_at"] < signal_at <= final["child_started_at"]
    assert final["child_started_at"] <= final["heartbeat_at"] <= after["observed_at"]
    assert after["observed_at"] - final["heartbeat_at"] < 15
    # K3: the container stayed up, including every unrelated container.
    for snapshot in (before, after):
        assert all(
            item["restartCount"] == 0 for item in snapshot["containers"].values()
        )

    start = before["containers"]["seal-default"]["state"]["running"]["startedAt"]
    assert (
        start
        and after["containers"]["seal-default"]["state"]["running"]["startedAt"]
        == start
    )
    # K4: Kubernetes' transition timestamp detects even a transient NotReady.
    initial, final = before["ready"], after["ready"]
    assert initial["status"] == final["status"] == "True"
    assert initial["lastTransitionTime"]
    assert initial["lastTransitionTime"] == final["lastTransitionTime"]
    # K5: real Service HTTP and ready endpoints bracket the OOM and recovery.
    assert before["pod_ip"] and after["pod_ip"] == before["pod_ip"]
    assert len(samples) >= 5
    assert any(
        sample.get("finished_at", float("inf")) < signal_at for sample in samples
    )
    assert any(sample.get("at", 0) > recovered_at for sample in samples)
    for sample in samples:
        assert "error" not in sample
        assert sample.get("at", 0) > 0 and sample["finished_at"] >= sample["at"]
        assert before["pod_ip"] in sample.get("ready_ips", [])
        assert sample.get("http") == 200

    # K6: the same broker remains ready in the same pod.
    assert before["uid"] and after["uid"] == before["uid"]
    for snapshot in (before, after):
        broker = snapshot["containers"]["broker"]
        assert broker["ready"] is True and broker["restartCount"] == 0


@pytest.fixture
def oom_observations():
    roles = [f"{role}-default" for role in STOP_ORDER]
    before: dict[str, Any] = {
        "uid": "11111111-2222-3333-4444-555555555555",
        "pod_ip": "10.0.0.10",
        "ready": {"status": "True", "lastTransitionTime": "2026-01-01T00:00:01Z"},
        "containers": {
            name: {
                "restartCount": 0,
                "ready": True,
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}},
            }
            for name in ["broker", *roles]
        },
        "oom_groups": dict.fromkeys(roles, 0),
        "oom_kills": 0,
        "supervisor": {
            "pid": 20,
            "restarts": 0,
            "last_exit": None,
            "child_started_at": 100,
            "heartbeat_at": 109,
        },
        "observed_at": 109,
    }
    after = copy.deepcopy(before)
    after.update(oom_kills=1, observed_at=121)
    after["supervisor"].update(
        pid=30,
        restarts=1,
        child_started_at=119,
        heartbeat_at=120,
        last_exit={"signal": 9, "oom": True},
    )
    samples = [
        {"at": at, "finished_at": at + 0.2, "http": 200, "ready_ips": ["10.0.0.10"]}
        for at in range(109, 123)
    ]
    return before, after, roles, samples, 110, 121


def test_pod_json_parsing_and_role_oom_isolation(oom_observations):
    before = oom_observations[0]
    parsed = pod_state(
        {
            "metadata": {"uid": before["uid"]},
            "status": {
                "podIP": before["pod_ip"],
                "conditions": [{"type": "Ready", **before["ready"]}],
                "containerStatuses": [
                    {"name": "broker", **before["containers"]["broker"]}
                ],
                "initContainerStatuses": [
                    {"name": name, **item}
                    for name, item in before["containers"].items()
                    if name != "broker"
                ],
            },
        }
    )
    assert parsed["containers"] == {
        name: {"name": name, **item} for name, item in before["containers"].items()
    }
    assert parsed["ready"] == {"type": "Ready", **before["ready"]}
    assert parsed["uid"] == before["uid"]
    assert_role_oom_isolated(*oom_observations)


@pytest.mark.parametrize(
    "violation",
    [
        "oom",
        "oom-extra",
        "pid",
        "role-count",
        "reason",
        "signal",
        "heartbeat",
        "child-start",
        "container",
        "container-start",
        "empty-container-start",
        "transition",
        "empty-transition",
        "ready",
        "zero-samples",
        "non-bracketing",
        "no-before-sample",
        "empty-containers",
        "empty-roles",
        "empty-groups",
        "oom-group",
        "empty-endpoints",
        "http",
        "broker",
        "uid",
        "pod-ip",
    ],
)
def test_pod_json_rejects_oom_isolation_violations(oom_observations, violation):
    before, after, roles, samples, signal_at, recovered_at = oom_observations
    if violation == "oom":
        after["oom_kills"] = 0
    elif violation == "oom-extra":
        after["oom_kills"] = 2
    elif violation == "pid":
        after["supervisor"]["pid"] = before["supervisor"]["pid"]
    elif violation == "role-count":
        after["supervisor"]["restarts"] = 0
    elif violation == "reason":
        after["supervisor"]["last_exit"]["oom"] = False
    elif violation == "signal":
        after["supervisor"]["last_exit"]["signal"] = 15
    elif violation == "heartbeat":
        after["supervisor"]["heartbeat_at"] = 99
    elif violation == "child-start":
        after["supervisor"]["child_started_at"] = 99
    elif violation == "container":
        after["containers"]["clean-default"]["restartCount"] = 1
    elif violation == "container-start":
        after["containers"]["seal-default"]["state"]["running"]["startedAt"] = "later"
    elif violation == "empty-container-start":
        before["containers"]["seal-default"]["state"]["running"]["startedAt"] = ""
        after["containers"]["seal-default"]["state"]["running"]["startedAt"] = ""
    elif violation == "transition":
        after["ready"]["lastTransitionTime"] = "later"
    elif violation == "empty-transition":
        before["ready"]["lastTransitionTime"] = ""
        after["ready"]["lastTransitionTime"] = ""
    elif violation == "ready":
        after["ready"]["status"] = "False"
    elif violation == "zero-samples":
        samples.clear()
    elif violation == "non-bracketing":
        samples[:] = samples[:5]
    elif violation == "no-before-sample":
        samples[:] = samples[-5:]
    elif violation == "empty-containers":
        after["containers"].clear()
    elif violation == "empty-roles":
        roles.clear()
    elif violation == "empty-groups":
        after["oom_groups"].clear()
    elif violation == "oom-group":
        after["oom_groups"]["seal-default"] = 1
    elif violation == "empty-endpoints":
        samples[0]["ready_ips"] = []
    elif violation == "http":
        samples[0]["http"] = 503
    elif violation == "broker":
        after["containers"]["broker"]["ready"] = False
    elif violation == "uid":
        after["uid"] = "different"
    elif violation == "pod-ip":
        after["pod_ip"] = ""

    with pytest.raises(AssertionError):
        assert_role_oom_isolated(before, after, roles, samples, signal_at, recovered_at)


@pytest.mark.parametrize("timeout", [None, 25])
def test_role_liveness_allows_cli_startup_under_contention(tmp_path, timeout):
    values = (
        {}
        if timeout is None
        else {"maintenance": {"liveness": {"timeoutSeconds": timeout}}}
    )
    roles = pod(render(tmp_path, values))["initContainers"]
    for role in roles:
        probe = role["livenessProbe"]
        assert probe["timeoutSeconds"] == (15 if timeout is None else timeout)
        assert probe["periodSeconds"] == 10
        assert probe["failureThreshold"] == 6


def kubectl(*args, input=None):
    timeout = 30
    if args[0] == "wait":
        # Waits below specify seconds. Let kubectl report its own deadline
        # before Python terminates it, including API setup and shutdown time.
        for arg in args:
            if arg.startswith("--timeout="):
                timeout += float(arg.removeprefix("--timeout=").removesuffix("s"))

    return subprocess.run(
        ["kubectl", *args],
        input=input,
        text=True,
        capture_output=True,
        check=True,
        timeout=timeout,
    ).stdout


@pytest.mark.parametrize(
    ("args", "expected_timeout"),
    [
        (("wait", "job/provision", "--timeout=90s"), 120),
        (("wait", "pod/http-client", "--timeout=60s"), 90),
        (("get", "pod", "broker"), 30),
        (("exec", "broker", "--", "cat", "/status"), 30),
        (("delete", "namespace", "chart-oom", "--wait=false"), 30),
    ],
)
def test_kubectl_timeout_allows_wait_budget(monkeypatch, args, expected_timeout):
    def run(command, **kwargs):
        assert command == ["kubectl", *args]
        assert kwargs["timeout"] == expected_timeout
        assert kwargs["check"] is True
        assert kwargs["input"] == "manifest"
        return subprocess.CompletedProcess(command, 0, stdout="observed")

    monkeypatch.setattr(subprocess, "run", run)
    assert kubectl(*args, input="manifest") == "observed"


@pytest.fixture
def external_scale_chart(tmp_path):
    image = os.environ["STREAMCAST_CHART_IMAGE"]
    repository, tag = image.rsplit(":", 1)
    namespace = "chart-scale-" + uuid.uuid4().hex[:8]
    values = {
        "fullnameOverride": "broker",
        "scaleOwner": "external",
        "image": {"repository": repository, "tag": tag, "pullPolicy": "Never"},
        "persistence": {"existingClaim": "data"},
        "broker": {
            "config": {
                "streams": [
                    {
                        "spec": {
                            "name": "events",
                            "schema": {
                                "type": "object",
                                "properties": {"value": {"type": "integer"}},
                                "required": ["value"],
                            },
                        },
                        "storage_profile": "default",
                    }
                ]
            }
        },
    }
    values_path = tmp_path / "scale-values.yaml"
    values_path.write_text(yaml.safe_dump(values))
    kubectl("create", "namespace", namespace)
    try:
        yield namespace, image, values_path
    finally:
        kubectl("delete", "namespace", namespace, "--wait=false")
        kubectl("delete", "pv", namespace, "--ignore-not-found", "--wait=false")


def provision_scale_claim(namespace, image):
    # Provision a real stream before starting the broker: the chart opens
    # existing state, so an empty hostPath would test a startup refusal instead.
    kubectl(
        "apply",
        "-f",
        "-",
        input=yaml.safe_dump_all(
            [
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": {"name": namespace},
                    "spec": {
                        "capacity": {"storage": "1Gi"},
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "",
                        "persistentVolumeReclaimPolicy": "Retain",
                        "claimRef": {"name": "data", "namespace": namespace},
                        "hostPath": {
                            "path": "/var/" + namespace,
                            "type": "DirectoryOrCreate",
                        },
                    },
                },
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": "data", "namespace": namespace},
                    "spec": {
                        "volumeName": namespace,
                        "storageClassName": "",
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "1Gi"}},
                    },
                },
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {"name": "provision", "namespace": namespace},
                    "spec": {
                        "backoffLimit": 0,
                        "template": {
                            "spec": {
                                "restartPolicy": "Never",
                                "containers": [
                                    {
                                        "name": "provision",
                                        "image": image,
                                        "imagePullPolicy": "Never",
                                        "securityContext": {"runAsUser": 0},
                                        "command": [
                                            "python",
                                            "-c",
                                            "import asyncio; import os; from pathlib import Path; "
                                            "from streamcast import Storage, Stream, StreamSpec, StreamHandle; "
                                            "root = Path('/var/lib/streamcast'); "
                                            "stream = Stream(spec=StreamSpec(name='events', schema={'type':'object','properties':{'value':{'type':'integer'}},'required':['value']}), storage=Storage(root=root)); "
                                            "asyncio.run(StreamHandle.open(stream, initialization='create_if_absent').aclose()); "
                                            "[os.chown(p, 10001, 10001) for p in [root, *root.rglob('*')]]",
                                        ],
                                        "volumeMounts": [
                                            {
                                                "name": "data",
                                                "mountPath": "/var/lib/streamcast",
                                            }
                                        ],
                                    }
                                ],
                                "volumes": [
                                    {
                                        "name": "data",
                                        "persistentVolumeClaim": {"claimName": "data"},
                                    }
                                ],
                            }
                        },
                    },
                },
            ]
        ),
    )
    kubectl(
        "wait",
        "-n",
        namespace,
        "job/provision",
        "--for=condition=complete",
        "--timeout=90s",
    )


def scale_helm_command(action, namespace, values_path):
    return [
        HELM,
        action,
        "scale",
        str(CHART),
        "-n",
        namespace,
        "-f",
        str(values_path),
        "--wait",
        "--timeout=90s",
    ]


def scale_workload(namespace):
    return json.loads(
        kubectl("get", "statefulset/broker", "-n", namespace, "-o", "json")
    )


def assert_scale_stopped(namespace):
    assert scale_workload(namespace)["spec"]["replicas"] == 0
    kubectl("wait", "-n", namespace, "pod/broker-0", "--for=delete", "--timeout=60s")
    assert not kubectl(
        "get", "pod/broker-0", "-n", namespace, "--ignore-not-found", "-o", "name"
    ).strip()


def start_scale_broker(namespace):
    kubectl("scale", "statefulset/broker", "-n", namespace, "--replicas=1")
    kubectl("wait", "-n", namespace, "pod/broker-0", "--for=create", "--timeout=30s")
    kubectl(
        "wait",
        "-n",
        namespace,
        "pod/broker-0",
        "--for=condition=Ready",
        "--timeout=120s",
    )
    assert scale_workload(namespace)["spec"]["replicas"] == 1
    observed = json.loads(kubectl("get", "pod/broker-0", "-n", namespace, "-o", "json"))
    volume = next(
        item for item in observed["spec"]["volumes"] if item["name"] == "data"
    )
    assert volume["persistentVolumeClaim"]["claimName"] == "data"
    return observed


@pytest.mark.skipif(
    os.getenv("STREAMCAST_CHART_KIND") != "1", reason="requires the dedicated kind job"
)
@pytest.mark.timeout(360)
def test_external_scale_installs_and_upgrades_without_claim(external_scale_chart):
    namespace, image, values_path = external_scale_chart
    install = subprocess.Popen(
        scale_helm_command("install", namespace, values_path),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        # Observe creation before acting as the external scale owner. The
        # claim stays absent throughout both Helm waits.
        kubectl(
            "wait",
            "-n",
            namespace,
            "statefulset/broker",
            "--for=create",
            "--timeout=30s",
        )
        kubectl("scale", "statefulset/broker", "-n", namespace, "--replicas=0")
        output, _ = install.communicate(timeout=120)
        assert install.returncode == 0, output
    finally:
        if install.poll() is None:
            install.kill()
            install.communicate(timeout=10)

    assert_scale_stopped(namespace)
    assert not json.loads(kubectl("get", "pvc", "-n", namespace, "-o", "json"))["items"]
    subprocess.run(
        [
            *scale_helm_command("upgrade", namespace, values_path),
            "--set-string=podAnnotations.scale-test=absent-claim",
        ],
        check=True,
        timeout=120,
    )
    workload = scale_workload(namespace)
    assert (
        workload["spec"]["template"]["metadata"]["annotations"]["scale-test"]
        == "absent-claim"
    )
    assert_scale_stopped(namespace)
    assert not json.loads(kubectl("get", "pvc", "-n", namespace, "-o", "json"))["items"]
    provision_scale_claim(namespace, image)
    start_scale_broker(namespace)


@pytest.mark.skipif(
    os.getenv("STREAMCAST_CHART_KIND") != "1", reason="requires the dedicated kind job"
)
@pytest.mark.timeout(360)
def test_external_scale_survives_helm_upgrade(external_scale_chart):
    namespace, image, values_path = external_scale_chart
    provision_scale_claim(namespace, image)
    subprocess.run(
        scale_helm_command("install", namespace, values_path), check=True, timeout=120
    )
    kubectl("scale", "statefulset/broker", "-n", namespace, "--replicas=0")
    assert_scale_stopped(namespace)
    subprocess.run(
        [
            *scale_helm_command("upgrade", namespace, values_path),
            "--set-string=podAnnotations.scale-test=stopped",
        ],
        check=True,
        timeout=120,
    )
    assert_scale_stopped(namespace)
    before = start_scale_broker(namespace)
    subprocess.run(
        [
            *scale_helm_command("upgrade", namespace, values_path),
            "--set-string=podAnnotations.scale-test=running",
        ],
        check=True,
        timeout=120,
    )
    assert scale_workload(namespace)["spec"]["replicas"] == 1
    after = json.loads(kubectl("get", "pod/broker-0", "-n", namespace, "-o", "json"))
    assert after["metadata"]["uid"] != before["metadata"]["uid"]
    assert after["metadata"]["annotations"]["scale-test"] == "running"
    assert any(
        item["type"] == "Ready" and item["status"] == "True"
        for item in after["status"]["conditions"]
    )


@pytest.mark.skipif(
    os.getenv("STREAMCAST_CHART_KIND") != "1", reason="requires the dedicated kind job"
)
@pytest.mark.timeout(360)
def test_oom_one_role_restarts_in_place_and_pod_ready_holds(tmp_path):
    # Allocate inside the maintainer itself: an exec helper's OOM could kill
    # only the helper and leave the role alive, proving no restart isolation.
    image = os.environ["STREAMCAST_CHART_IMAGE"]
    repository, tag = image.rsplit(":", 1)
    namespace = "chart-oom"
    kubectl("create", "namespace", namespace)
    kubectl(
        "apply",
        "-f",
        "-",
        input=yaml.safe_dump_all(
            [
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": {"name": "chart-oom"},
                    "spec": {
                        "capacity": {"storage": "1Gi"},
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "",
                        "hostPath": {
                            "path": "/var/chart-oom",
                            "type": "DirectoryOrCreate",
                        },
                    },
                },
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": "data", "namespace": namespace},
                    "spec": {
                        "volumeName": "chart-oom",
                        "storageClassName": "",
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "1Gi"}},
                    },
                },
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {"name": "provision", "namespace": namespace},
                    "spec": {
                        "backoffLimit": 0,
                        "template": {
                            "spec": {
                                "restartPolicy": "Never",
                                "containers": [
                                    {
                                        "name": "provision",
                                        "image": image,
                                        "imagePullPolicy": "Never",
                                        "securityContext": {"runAsUser": 0},
                                        "command": [
                                            "python",
                                            "-c",
                                            "from pathlib import Path; import asyncio; import os; from streamcast import Storage, Stream, StreamSpec, StreamHandle; "
                                            "stream = Stream(spec=StreamSpec(name='events', schema={'type':'object','properties':{'value':{'type':'integer'}},'required':['value']}), storage=Storage(root=Path('/var/lib/streamcast'))); "
                                            "asyncio.run(StreamHandle.open(stream, initialization='create_if_absent').aclose()); "
                                            "[(os.chown(p, 10001, 10001)) for p in [Path('/var/lib/streamcast'), *Path('/var/lib/streamcast').rglob('*')]]",
                                        ],
                                        "volumeMounts": [
                                            {
                                                "name": "data",
                                                "mountPath": "/var/lib/streamcast",
                                            }
                                        ],
                                    }
                                ],
                                "volumes": [
                                    {
                                        "name": "data",
                                        "persistentVolumeClaim": {"claimName": "data"},
                                    }
                                ],
                            }
                        },
                    },
                },
            ]
        ),
    )
    try:
        kubectl(
            "wait",
            "-n",
            namespace,
            "--for=condition=complete",
            "job/provision",
            "--timeout=90s",
        )
        values = {
            "image": {"repository": repository, "tag": tag, "pullPolicy": "Never"},
            "persistence": {"existingClaim": "data"},
            "broker": {
                "config": {
                    "streams": [
                        {
                            "spec": {
                                "name": "events",
                                "schema": {
                                    "type": "object",
                                    "properties": {"value": {"type": "integer"}},
                                    "required": ["value"],
                                },
                            },
                            "storage_profile": "default",
                        }
                    ]
                }
            },
            "maintenance": {
                "command": [
                    "python",
                    "-c",
                    "import runpy, signal, sys; "
                    "signal.signal(signal.SIGUSR1, lambda *_: bytearray(2**31)); "
                    "sys.argv = ['streamcast', 'maintain', '--follow', '/run/streamcast', *sys.argv[1:]]; "
                    "runpy.run_module('streamcast', run_name='__main__')",
                ]
            },
        }
        values_path = tmp_path / "kind-values.yaml"
        values_path.write_text(yaml.safe_dump(values))
        subprocess.run(
            [
                HELM,
                "install",
                "oom",
                str(CHART),
                "-n",
                namespace,
                "-f",
                str(values_path),
                "--wait",
                "--timeout",
                "120s",
            ],
            check=True,
        )
        target = "oom-streamcast-0"
        expected_roles = [
            item["name"] for item in pod(render(tmp_path, values))["initContainers"]
        ]
        assert (
            len(expected_roles)
            == 5
            * len(values.get("maintenance", {}).get("groups", [{"name": "default"}]))
            > 0
        )

        def execute(container, *command):
            return kubectl(
                "exec", "-n", namespace, target, "-c", container, "--", *command
            )

        def observation():
            result = pod_state(
                json.loads(kubectl("get", "pod", target, "-n", namespace, "-o", "json"))
            )
            result["oom_groups"] = {
                role: int(execute(role, "cat", "/sys/fs/cgroup/memory.oom.group"))
                for role in expected_roles
            }
            result.update(
                json.loads(
                    execute(
                        "seal-default",
                        "python",
                        "-c",
                        "import json; from pathlib import Path; "
                        "events = dict(line.split() for line in Path('/sys/fs/cgroup/memory.events').read_text().splitlines()); "
                        "print(json.dumps({'oom_kills': int(events['oom_kill']), "
                        "'supervisor': json.loads(Path('/run/streamcast/seal-default.supervise.json').read_text())}))",
                    )
                )
            )
            result["observed_at"] = time.time()
            return result

        execute(
            "seal-default",
            "streamcast",
            "probe",
            "role",
            "--run-dir",
            "/run/streamcast",
            "--role",
            "seal",
            "--group",
            "default",
        )
        before = observation()
        assert_single_process_oom(before, expected_roles)
        kubectl(
            "run",
            "http-client",
            "-n",
            namespace,
            "--image=" + image,
            "--image-pull-policy=Never",
            "--restart=Never",
            "--command",
            "--",
            "python",
            "-c",
            "import time; time.sleep(3600)",
        )
        kubectl(
            "wait",
            "-n",
            namespace,
            "pod/http-client",
            "--for=condition=Ready",
            "--timeout=60s",
        )
        service_ip = kubectl(
            "get",
            "service",
            "oom-streamcast",
            "-n",
            namespace,
            "-o",
            "jsonpath={.spec.clusterIP}",
        ).strip()
        assert service_ip and service_ip != "None"
        samples = []
        stop, sampled = threading.Event(), threading.Event()

        def sample_service():
            while not stop.is_set():
                started = time.monotonic()
                sample = {"at": time.time()}
                try:
                    slices = json.loads(
                        kubectl(
                            "get",
                            "endpointslices",
                            "-n",
                            namespace,
                            "-l",
                            "kubernetes.io/service-name=oom-streamcast",
                            "-o",
                            "json",
                        )
                    )["items"]
                    sample["ready_ips"] = [
                        address
                        for item in slices
                        for endpoint in item["endpoints"]
                        if endpoint.get("conditions", {}).get("ready") is True
                        for address in endpoint["addresses"]
                    ]
                    sample["http"] = int(
                        kubectl(
                            "exec",
                            "-n",
                            namespace,
                            "http-client",
                            "--",
                            "python",
                            "-c",
                            "import urllib.request; "
                            f"print(urllib.request.urlopen('http://{service_ip}:8765/readyz', timeout=3).status)",
                        )
                    )
                except (subprocess.SubprocessError, ValueError, KeyError) as exc:
                    sample["error"] = type(exc).__name__

                sample["finished_at"] = time.time()
                samples.append(sample)
                sampled.set()
                stop.wait(max(0, 1 - (time.monotonic() - started)))

        sampler = threading.Thread(target=sample_service)
        sampler.start()
        try:
            assert sampled.wait(20), "no Service sample before the signal"
            signal_at = time.time()
            execute(
                "seal-default",
                "python",
                "-c",
                f"import os, signal; os.kill({before['supervisor']['pid']}, signal.SIGUSR1)",
            )
            deadline = time.monotonic() + 120
            while True:
                after = observation()
                status = after["supervisor"]
                if (
                    status.get("pid")
                    and status["pid"] != before["supervisor"]["pid"]
                    and status["restarts"] == before["supervisor"]["restarts"] + 1
                    and status["last_exit"]
                    == {"code": None, "signal": 9, "oom": True, "liveness": False}
                    and signal_at
                    <= status["child_started_at"]
                    <= status["heartbeat_at"]
                    and after["observed_at"] - status["heartbeat_at"] < 15
                ):
                    recovered_at = time.time()
                    break

                assert time.monotonic() < deadline, "role child never restarted"
                time.sleep(0.2)

            # Keep sampling after K2; assert count and both brackets below.
            deadline = time.monotonic() + 30
            while len(samples) < 5 or not any(s["at"] > recovered_at for s in samples):
                assert time.monotonic() < deadline, (
                    "missing post-recovery Service samples"
                )
                time.sleep(0.2)

        finally:
            stop.set()
            sampler.join(timeout=35)
            assert not sampler.is_alive(), "Service sampler did not finish"

        after = observation()
        assert_role_oom_isolated(
            before, after, expected_roles, samples, signal_at, recovered_at
        )
    finally:
        kubectl("delete", "namespace", namespace, "--wait=false")
        kubectl("delete", "pv", "chart-oom", "--wait=false")
