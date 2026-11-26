"""Render real manifests; the opt-in kind tier exercises kubelet OOM isolation."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tomllib
from pathlib import Path

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
        assert item["command"] == [
            "streamcast",
            "maintain",
            "--follow",
            "/run/streamcast",
        ]
        assert item["livenessProbe"]["exec"]["command"] == [
            "streamcast",
            "probe",
            "role",
            "--run-dir",
            "/run/streamcast",
            "--role",
            role,
            "--group",
            group,
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


@pytest.mark.parametrize("replicas", [0, 2, 3])
def test_replicas_other_than_one_fails(tmp_path, replicas):
    result = render(tmp_path, {"replicas": replicas}, check=False)
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


def kubectl(*args, input=None):
    return subprocess.run(
        ["kubectl", *args], input=input, text=True, capture_output=True, check=True
    ).stdout


@pytest.mark.skipif(
    os.getenv("STREAMCAST_CHART_KIND") != "1", reason="requires the dedicated kind job"
)
@pytest.mark.timeout(360)
def test_oom_one_role_restarts_only_that_container(tmp_path):
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
        before = json.loads(
            kubectl("get", "pod", target, "-n", namespace, "-o", "json")
        )
        statuses = before["status"]["initContainerStatuses"]
        seal = next(item for item in statuses if item["name"] == "seal-default")
        kubectl(
            "exec",
            "-n",
            namespace,
            target,
            "-c",
            "seal-default",
            "--",
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
        kubectl(
            "exec",
            "-n",
            namespace,
            target,
            "-c",
            "seal-default",
            "--",
            "python",
            "-c",
            "import os, signal; os.kill(1, signal.SIGUSR1)",
        )
        kubectl(
            "wait",
            "-n",
            namespace,
            f"pod/{target}",
            f'--for=jsonpath={{.status.initContainerStatuses[?(@.name=="seal-default")].restartCount}}={seal["restartCount"] + 1}',
            "--timeout=120s",
        )
        after = json.loads(kubectl("get", "pod", target, "-n", namespace, "-o", "json"))
        assert after["metadata"]["uid"] == before["metadata"]["uid"]
        initial = {item["name"]: item for item in before["status"]["conditions"]}[
            "Ready"
        ]
        final = {item["name"]: item for item in after["status"]["conditions"]}["Ready"]
        assert initial["status"] == final["status"] == "True"
        assert initial["lastTransitionTime"] == final["lastTransitionTime"]
        counts = {
            item["name"]: item["restartCount"]
            for item in after["status"]["containerStatuses"]
            + after["status"]["initContainerStatuses"]
        }
        assert counts["seal-default"] > seal["restartCount"]
        assert all(
            count == 0 for name, count in counts.items() if name != "seal-default"
        )
        failed = next(
            item
            for item in after["status"]["initContainerStatuses"]
            if item["name"] == "seal-default"
        )
        assert failed["lastState"]["terminated"]["reason"] == "OOMKilled"
        kubectl(
            "exec",
            "-n",
            namespace,
            target,
            "-c",
            "broker",
            "--",
            "streamcast",
            "probe",
            "ready",
            "--url",
            "http://127.0.0.1:8765",
        )
    finally:
        kubectl("delete", "namespace", namespace, "--wait=false")
        kubectl("delete", "pv", "chart-oom", "--wait=false")
