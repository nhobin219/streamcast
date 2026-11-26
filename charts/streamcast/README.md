# streamcast chart

One release runs one broker and five native maintenance sidecars per credential
group on Kubernetes 1.29 or newer. Every container shares the data volume and its
aliases. Sidecars stop in the order seal, publish, compact, clean, clean-published,
after the broker. Only the broker's readiness controls Service membership.
Maintenance liveness probes allow 15 seconds for CLI startup under contention;
`maintenance.liveness` configures that timeout, the 10-second period and six
consecutive failures before restart. Longer timeouts can delay detection of a
hung role.

The chart source at a release tag is the versioned artifact; vendor this directory
at that tag. Chart version and appVersion follow the Python package version. Set
`image.repository` to the built image's repository and `image.tag` to that version.
There is no image pull or chart publication prerequisite for a locally built image.

```sh
helm lint charts/streamcast --strict
helm template broker charts/streamcast --values values.yaml
helm install broker charts/streamcast --values values.yaml
```

Configure `broker.config` with BrokerConfig fields, plus `storage_profiles` and
`streams` from the service configuration. The chart places the first set under
`[broker]`, renders the other two at the TOML root, and sets the server to
`0.0.0.0` on `broker.port`. Maintenance policy fields go in `broker.config.maintain`.
The chart sets its `external.run_dir` and `external.groups` from `maintenance`.
Each group names a storage profile. Group names are unique DNS labels of at most
40 characters, so the container name can include both role and group.

Supply existing static streams or an explicitly provisioned registry; defaults
alone do not provision a broker. Startup opens existing state. The chart never
initializes a registry, creates a stream, or creates a Secret. For example, a
registry-backed configuration uses:

```yaml
image:
  repository: streamcast
  tag: "0.29.1"
broker:
  authorization: streamcast:TokenFileAccess.from_env
  config:
    registry: /var/lib/streamcast/registry.db
    storage_profiles:
      default:
        root: /var/lib/streamcast
  env:
    - name: STREAMCAST_CONTROL_TOKEN_FILE
      value: /control/token
extraVolumes:
  - name: control
    persistentVolumeClaim:
      claimName: broker-control
extraVolumeMounts:
  - name: control
    mountPath: /control
    readOnly: true
```

`TokenFileAccess` requires a nonempty 0600 file readable by the broker's UID.
Prepare that file with `streamcast token-file create` and mount it through
`extraVolumes`/`extraVolumeMounts`; those extra mounts reach every container.
The example uses an existing claim containing a token owned by UID 10001. Token
provisioning and registry initialization happen explicitly before installation.
Registry mode without `broker.authorization` fails during rendering. With
`broker.command` set, the chart omits the ConfigMap and its validation; the custom
application supplies authorization, storage profiles and ExternalMaintain itself.

`broker.image` overrides the base image; maintenance inherits that image unless
`maintenance.image` overrides it. Use identical streamcast/litelink versions in
every container. `maintenance.command` accepts a base command; the chart appends
`--role` and `--group`. Give each group its own `env`/`envFrom`. Broker environment
variables are not copied into roles. No credentials belong in `broker.config`.

New data claims use ReadWriteOncePod and retain their data on scale-down and
deletion. `persistence.existingClaim` omits the claim template and adopts the
claim's existing access mode. `persistence.aliasMounts` mounts the same volume at
absolute catalog paths. Never attach the same data volume to two brokers.
The run directory is a 16 MiB memory emptyDir; shared temporary space is an 8 GiB
emptyDir. Both disappear with the pod. Per-role resources and both bounds are
configurable. Read-only root filesystems leave only mounted paths writable.

Use `placement.nodeSelector`, `tolerations`, `affinity` and
`topologySpreadConstraints` for placement. To prefer Spot with on-demand fallback,
select a node pool that admits both capacity types and prefers Spot:

```yaml
placement:
  nodeSelector:
    karpenter.sh/nodepool: system
```

Do not add a `karpenter.sh/capacity-type: spot` selector to that configuration: it
prevents on-demand fallback. To require on-demand capacity, select an on-demand
pool or explicitly set `karpenter.sh/capacity-type: on-demand`.

Set `service.enabled: false` when an existing Service selects `podLabels`.
`broker.portName` defaults to `http`; a parent can set a role-specific name.
The chart's reserved `app.kubernetes.io/name` and `streamcast.io/broker` labels
keep StatefulSet selectors distinct across aliases while allowing parent labels.
`serviceAccount` supports an existing account and annotations for ambient identity;
token automount defaults to false. `podAnnotations`, `priorityClassName` and
`terminationGracePeriodSeconds` pass through to the pod.

Chart CI runs lint, template assertions and a kind install on hosted runners. The
kind test uses a local image and an adopted test volume. A signal handler in the
test's role command allocates beyond seal's limit inside the maintainer process,
forcing a real cgroup OOM. The test checks that only seal restarts, the pod UID and
Ready transition stay unchanged, and the broker's ready probe still answers.
