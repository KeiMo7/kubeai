# Storage / Caching

With "Large" in the name, caching is a critical part of serving LLMs.

The best caching technique may very depending on your environment:

* What cloud features are available?
* Is your cluster deployed in an air-gapped environment?

## A. Model built into container

**Status:** Supported

Building a model into a container image can provide a simple way to take advantage of image-related optimizations built into Kubernetes:

* Relaunching a model server on the same Node that it ran on before will [likely](https://kubernetes.io/docs/concepts/architecture/garbage-collection/#container-image-lifecycle) be able to reuse the previously pulled image.

* [Secondary boot disks on GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/data-container-image-preloading) can be used to avoid needing to pull images.

* [Image streaming on GKE](https://cloud.google.com/blog/products/containers-kubernetes/introducing-container-image-streaming-in-gke) can allow for containers to startup before the entire image is present on the Node.

* Container images can be pre-installed on Nodes in air-gapped environments (example: [k3s airgap installation](https://docs.k3s.io/installation/airgap)).


**Guides:**

* [How to build models into container images](../how-to/build-models-into-containers.md)

## B. Model on shared filesystem (read-write-many)

KubeAI can manage model caches on a shared filesystem (i.e. AWS [EFS](https://aws.amazon.com/efs/), GCP [Filestore](https://cloud.google.com/filestore/docs/overview), NFS). It manages the full lifecycle of a cached model: loading, serving, and cache eviction (on deletion of the Model).

<br>
<img src="/diagrams/caching-shared-filesystem.excalidraw.png" width="90%"></img>

### Cache entries and capacity

The shared volume is accounted for as a set of **cache entries**. An entry is
identified by a stable key derived from the source address and its version
identifier; Models that reference the same source version share one entry and
the weights are stored once. Recreating a Model object does not change the key,
and keys never contain source addresses, credentials or query parameters.

A new version (a different `oci://` tag, or an explicit
`cache.kubeai.org/version` annotation) creates a new entry. The
previous entry is left untouched and becomes reclaimable once no Model
references it and no replica is using it. An in-use entry is never overwritten.

The volume's storage request is its **budget**. Before a download Job is
created KubeAI checks `used + pending + new` against that budget. When it does
not fit, KubeAI first reclaims the least recently referenced entries that are
unreferenced and not served by any ready replica, then re-checks. If it still
does not fit, the load is rejected with the volume, the shortfall and the
entries occupying it; no download Job is started and the rejection is recorded
in status. Concurrent loads reserve capacity atomically on the volume ledger,
so two loads cannot both fit into the same free space.

Sizing notes:

* The measured size is read from the loader's termination message after a
  successful download and calibrated in the volume ledger.
* Before the first measurement, declare an expected size with the
  `cache.kubeai.org/expected-size-bytes: "<bytes>"` annotation on the Model to
  make the admission check strict. Without a declared or measured size the load
  is admitted and calibrated afterwards, so a volume with no declared capacity
  or an unmeasured entry is never blocked or deleted.

`status.cache` reports the entry (`key`, `sizeBytes`, `sizeKnown`, `lastUsed`,
`referencedBy`, `loadState`, `reclaimable`), the volume ledger (budget, used,
pending and the entry list) and the most recent admission rejections and
reclaims, so a full volume is visible instead of showing as zero replicas.

### Replica placement

For Models using a cache profile, `status.cache.placement` records each
replica's node, the deterministic candidate preference order for replicas that
still need to be created, and the reason for the conclusion. Preferences only
order nodes that already satisfy the node selector, required affinity,
tolerations and runtime requirements; when node inventory is unavailable the
conclusion is reported as invalid and replica creation falls back to the normal
scheduler. Preferences are recomputed only from cluster state and never change
the Pod template, so a ready replica is not recreated when the preference
changes.

## C. Model on read-only-many disk

**Status:** [Planned](https://github.com/kubeai-project/kubeai/blob/main/proposals/model-storage.md).

Examples: [GCP Hyperdisk ML](https://cloud.google.com/compute/docs/disks/hyperdisks)
