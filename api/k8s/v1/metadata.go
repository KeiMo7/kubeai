package v1

const (
	PodModelLabel = "model"
	// PodHashLabel is a label key used to store the hash of the Pod spec
	// that was used to create the Pod. This is used to determine if a Pod
	// needs to be recreated.
	PodHashLabel = "pod-hash"

	ModelFeatureLabelDomain = "features.kubeai.org"

	// ModelPodIPAnnotation is the annotation key used to specify an IP
	// to use for the model Pod instead of the IP address in the status of the Pod.
	// Use in conjunction with --allow-pod-address-override for development purposes.
	ModelPodIPAnnotation   = "model-pod-ip"
	ModelPodPortAnnotation = "model-pod-port"

	ModelCacheEvictionFinalizer = "kubeai.org/cache-eviction"

	// PVCCacheLedgerAnnotation stores the cache entry ledger for a single
	// cache volume. The ledger is derived from cluster state (Models, Pods,
	// Jobs and previously measured sizes) and is the only place where cache
	// entry reservations are persisted.
	PVCCacheLedgerAnnotation = "cache.kubeai.org/ledger"

	// PodCacheEntryLabel identifies the cache entry a Pod reads. It is only
	// set on Pods for Models that use a cache profile and contains the hashed
	// entry key so it never exposes a source URL.
	PodCacheEntryLabel = "cache.kubeai.org/entry"

	// ModelCacheSizeAnnotation optionally declares the expected size of a
	// cache entry in bytes. It is used to make admission decisions before a
	// source has ever been measured. Measured sizes always take precedence.
	ModelCacheSizeAnnotation = "cache.kubeai.org/expected-size-bytes"

	// ModelCacheVersionAnnotation pins the cache entry version. Changing it
	// makes the Model reference a new entry; existing entries are left in
	// place and become reclaimable once no Model references them.
	ModelCacheVersionAnnotation = "cache.kubeai.org/version"

	// CacheJobModelLabel marks cache load/evict Jobs with the owning Model so
	// Jobs for superseded entries are cleaned up together with the Model.
	CacheJobModelLabel = "cache.kubeai.org/model"
	// CacheJobRoleLabel identifies the role of a cache Job: load, evict or
	// reclaim.
	CacheJobRoleLabel = "cache.kubeai.org/job"
	// CacheJobEntryLabel marks a cache Job with the hashed entry it operates
	// on. It never contains a source URL or credential.
	CacheJobEntryLabel = "cache.kubeai.org/job-entry"
)

func PVCModelAnnotation(modelName string) string {
	return "models.kubeai.org/" + modelName
}

const (
	PodAdapterLabelPrefix = "adapter.kubeai.org/"
)

func PodAdapterLabel(adapterID string) string {
	return PodAdapterLabelPrefix + adapterID
}
