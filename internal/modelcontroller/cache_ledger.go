package modelcontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/kubeai-project/kubeai/internal/k8sutils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Cache load states surfaced in Model status.
const (
	cacheLoadStatePending = "Pending"
	cacheLoadStateLoading = "Loading"
	cacheLoadStateReady   = "Ready"
	cacheLoadStateFailed  = "Failed"
	cacheLoadStateUnknown = "Unknown"
)

const (
	// cacheHistoryLimit bounds the rejection/reclaim history kept per volume.
	cacheHistoryLimit = 5
	// cacheRetryDelay is how long the controller waits before retrying a
	// rejected admission or a load that needs another reconcile.
	cacheRetryDelay = 15 * time.Second
	// cacheStaleReservationAge bounds how long a reservation without a Job is
	// trusted. After this the next admission recomputes from measured state.
	cacheStaleReservationAge = 10 * time.Minute
	// cacheReclaimMissingJobGrace bounds how long a reclaim Job that was
	// recorded as running may stay missing before the entry is considered
	// reclaimed locally.
	cacheReclaimMissingJobGrace = 2 * time.Minute
	// cacheReclaimBackoff prevents retrying a failed reclaim on every
	// reconcile.
	cacheReclaimBackoff = 10 * time.Minute
)

const cacheLedgerVersion = 1

// cacheLedger is the persisted capacity ledger of a cache volume. It only
// records facts that cannot be derived from other cluster objects: the
// physical path assigned to an entry, the last calibration of its size, and
// in-flight reservations. Entry references and serving Pods are always derived
// from Models and Pods so the ledger can never disagree with them.
type cacheLedger struct {
	Version          int                                  `json:"version"`
	Entries          map[string]*cacheLedgerEntry         `json:"entries,omitempty"`
	Reservations     map[string]cacheLedgerReservation    `json:"reservations,omitempty"`
	RecentReclaims   []kubeaiv1.ModelCacheReclaimStatus   `json:"recentReclaims,omitempty"`
	RecentRejections []kubeaiv1.ModelCacheRejectionStatus `json:"recentRejections,omitempty"`
}

type cacheLedgerEntry struct {
	// RelPath is the entry directory relative to the volume root, for example
	// "models/my-model-<uid>". It is assigned once and shared by every Model
	// that references the entry.
	RelPath string `json:"relPath"`
	// SizeBytes is the measured size, or the declared estimate while no
	// measurement is available.
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// SizeKnown is true only after a measurement (or a test probe) succeeded.
	SizeKnown bool `json:"sizeKnown,omitempty"`
	// Present is true once the download completed successfully.
	Present bool `json:"present,omitempty"`
	// Partial is true when the last download attempt failed or was
	// interrupted and may have left files on the volume. It keeps the entry
	// from being treated as present or reclaimable until it is cleaned up.
	Partial bool `json:"partial,omitempty"`
	// Reclaiming is true while a reclaim Job is running for the entry.
	Reclaiming bool `json:"reclaiming,omitempty"`
	// ReclaimingSince bounds how long a missing reclaim Job is tolerated.
	ReclaimingSince *time.Time `json:"reclaimingSince,omitempty"`
	// ReclaimReason explains why the entry is being reclaimed.
	ReclaimReason string `json:"reclaimReason,omitempty"`
	// ReclaimAttempted is set when a reclaim failed and the entry may be
	// retried. It prevents an endless rapid retry loop.
	ReclaimAttempted *time.Time `json:"reclaimAttempted,omitempty"`
	// LastUsed is the last time the entry was loaded or served.
	LastUsed *time.Time `json:"lastUsed,omitempty"`
}

type cacheLedgerReservation struct {
	EntryKey  string    `json:"entryKey"`
	ModelName string    `json:"modelName"`
	ModelUID  string    `json:"modelUID"`
	JobName   string    `json:"jobName"`
	Bytes     int64     `json:"bytes,omitempty"`
	SizeKnown bool      `json:"sizeKnown,omitempty"`
	StartedAt time.Time `json:"startedAt"`
}

func newCacheLedger() *cacheLedger {
	return &cacheLedger{
		Version:      cacheLedgerVersion,
		Entries:      map[string]*cacheLedgerEntry{},
		Reservations: map[string]cacheLedgerReservation{},
	}
}

// parseCacheLedger decodes the ledger annotation. A missing or corrupt ledger
// is treated as empty so the controller cold-rebuilds instead of error
// looping; no entry directory is ever deleted as a result.
func parseCacheLedger(pvc *corev1.PersistentVolumeClaim) *cacheLedger {
	raw := k8sutils.GetAnnotation(pvc, kubeaiv1.PVCCacheLedgerAnnotation)
	if raw == "" {
		return newCacheLedger()
	}
	l := newCacheLedger()
	if err := json.Unmarshal([]byte(raw), l); err != nil {
		return newCacheLedger()
	}
	if l.Entries == nil {
		l.Entries = map[string]*cacheLedgerEntry{}
	}
	if l.Reservations == nil {
		l.Reservations = map[string]cacheLedgerReservation{}
	}
	return l
}

func (l *cacheLedger) pathUsed(relPath string) bool {
	for _, e := range l.Entries {
		if e != nil && e.RelPath == relPath {
			return true
		}
	}
	return false
}

// cacheEntryView is the derived view of one entry: identity from the ledger,
// references and serving state from Models and Pods.
type cacheEntryView struct {
	Key         string
	RelPath     string
	SizeBytes   int64
	SizeKnown   bool
	Present     bool
	Partial     bool
	Reclaiming  bool
	LastUsed    time.Time
	References  []string
	ServingPods int
	LoadState   string
	LoadJob     string
}

func (e *cacheEntryView) referencedByCount() int32 {
	return int32(len(e.References))
}

// Reclaimable reports whether the entry may be deleted to make room. An entry
// that any Model references, that any Pod is using, or that has an in-flight
// load or reclaim is never reclaimable. A partial download is not reclaimable
// as capacity because its size is unknown; a separate cleanup removes it.
func (e *cacheEntryView) Reclaimable() bool {
	return e.Present && !e.Partial && !e.Reclaiming && len(e.References) == 0 && e.ServingPods == 0 && e.LoadJob == ""
}

// cacheVolumeView is the derived state of one cache volume.
type cacheVolumeView struct {
	Name         string
	BudgetBytes  int64
	BudgetKnown  bool
	Entries      map[string]*cacheEntryView
	Reservations map[string]cacheLedgerReservation
	Ledger       *cacheLedger
}

func (v *cacheVolumeView) sortedEntryKeys() []string {
	keys := make([]string, 0, len(v.Entries))
	for key := range v.Entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// buildCacheView derives the volume state from the cluster. Models, Pods and
// Jobs are the source of truth for references, serving and pending loads; the
// ledger only contributes physical paths, calibrated sizes and reservations.
// excludeModel is used while finalizing a Model so its own reference does not
// keep its entry alive.
func (r *ModelReconciler) buildCacheView(ctx context.Context, pvc *corev1.PersistentVolumeClaim, cacheProfile, excludeModel string) (*cacheVolumeView, error) {
	ledger := parseCacheLedger(pvc)
	view := &cacheVolumeView{
		Name:         pvc.Name,
		BudgetBytes:  cacheVolumeBudget(pvc),
		Entries:      map[string]*cacheEntryView{},
		Reservations: map[string]cacheLedgerReservation{},
		Ledger:       ledger,
	}
	view.BudgetKnown = view.BudgetBytes > 0

	// Reservations only count while their load Job is still active. A stale
	// reservation must never hold budget or appear pending.
	liveReservations := map[string]cacheLedgerReservation{}
	for key, res := range ledger.Reservations {
		if res.JobName != "" {
			job := &batchv1.Job{}
			err := r.Client.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: res.JobName}, job)
			switch {
			case err == nil && !k8sutils.IsJobCompleted(job) && !jobHasFailed(job):
				liveReservations[key] = res
				continue
			case apierrors.IsNotFound(err) && time.Since(res.StartedAt) < cacheStaleReservationAge:
				// The Job may not be visible yet; keep the reservation briefly
				// so two loads cannot both fit before the Job is observed.
				liveReservations[key] = res
				continue
			case err != nil && !apierrors.IsNotFound(err):
				liveReservations[key] = res
				continue
			}
			continue
		}
		if time.Since(res.StartedAt) < cacheStaleReservationAge {
			liveReservations[key] = res
		}
	}
	view.Reservations = liveReservations

	for key, le := range ledger.Entries {
		if le == nil {
			continue
		}
		entry := &cacheEntryView{
			Key:        key,
			RelPath:    le.RelPath,
			SizeBytes:  le.SizeBytes,
			SizeKnown:  le.SizeKnown,
			Present:    le.Present,
			Partial:    le.Partial,
			Reclaiming: le.Reclaiming,
		}
		if _, ok := liveReservations[key]; ok && !entry.Present {
			// A reservation means the entry is actively being fetched; it must
			// not be reused, reclaimed or reported ready.
			entry.LoadState = cacheLoadStateLoading
		}
		if le.LastUsed != nil {
			entry.LastUsed = *le.LastUsed
		}
		view.Entries[key] = entry
	}

	models := &kubeaiv1.ModelList{}
	if err := r.List(ctx, models, client.InNamespace(pvc.Namespace)); err != nil {
		return nil, fmt.Errorf("listing models for cache view: %w", err)
	}

	modelEntryKey := map[string]string{}
	for i := range models.Items {
		m := &models.Items[i]
		if m.Spec.CacheProfile != cacheProfile {
			continue
		}
		if excludeModel != "" && m.Name == excludeModel {
			continue
		}
		src, err := parseModelURL(m.Spec.URL)
		if err != nil {
			// Invalid sources are rejected by the API. Ignore them here so a
			// single bad object cannot stall the cache loop.
			continue
		}
		identity := cacheEntryIdentityForModel(m, src)
		modelEntryKey[m.Name] = identity.Key
		entry := view.Entries[identity.Key]
		if entry == nil {
			entry = &cacheEntryView{Key: identity.Key}
			view.Entries[identity.Key] = entry
		}
		entry.References = append(entry.References, m.Name)
		// Approximation for the reference recency used by reclaim ordering:
		// the Model object's creation time bounds when the entry was last
		// referenced. Serving Pods contribute a more precise value below.
		if t := m.CreationTimestamp.Time; t.After(entry.LastUsed) {
			entry.LastUsed = t
		}

		// Backward compatibility: a cache volume written by an older version
		// has no ledger entry but records a per-Model annotation for the
		// historic directory. Adopt it only when the ledger does not already
		// know this entry and the historic directory is still free; otherwise
		// a new source version would be mistaken for the old, present entry.
		if _, known := ledger.Entries[identity.Key]; known {
			continue
		}
		legacyRelPath := strings.TrimPrefix(modelCacheDir(m), "/")
		if ledger.pathUsed(legacyRelPath) {
			continue
		}
		ann, _ := parsePVCModelAnnotation(pvc, m.Name)
		if ann.UID == string(m.UID) {
			entry.Present = true
			if entry.RelPath == "" {
				entry.RelPath = legacyRelPath
			}
		}
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(pvc.Namespace)); err != nil {
		return nil, fmt.Errorf("listing pods for cache view: %w", err)
	}
	// Map physical entry paths to keys so a Pod still serving the previous
	// version during a rollout protects the entry it actually mounts instead
	// of the Model's new entry.
	keyByRelPath := map[string]string{}
	for key, le := range ledger.Entries {
		if le != nil && le.RelPath != "" {
			keyByRelPath[le.RelPath] = key
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		key := k8sutils.GetLabel(pod, kubeaiv1.PodCacheEntryLabel)
		if key == "" {
			if mounted := podCacheEntryRelPath(pod, pvc.Name); mounted != "" {
				key = keyByRelPath[mounted]
			}
		}
		if key == "" {
			// Pods created before this version carry no entry label. Derive
			// the key from the owning Model so they still protect the entry.
			key = modelEntryKey[k8sutils.GetLabel(pod, kubeaiv1.PodModelLabel)]
		}
		if key == "" {
			continue
		}
		entry := view.Entries[key]
		if entry == nil {
			entry = &cacheEntryView{Key: key}
			view.Entries[key] = entry
		}
		entry.ServingPods++
		if t := pod.CreationTimestamp.Time; t.After(entry.LastUsed) {
			entry.LastUsed = t
		}
	}

	jobs := &batchv1.JobList{}
	if err := r.List(ctx, jobs, client.InNamespace(pvc.Namespace)); err != nil {
		return nil, fmt.Errorf("listing jobs for cache view: %w", err)
	}
	jobsByName := make(map[string]*batchv1.Job, len(jobs.Items))
	for i := range jobs.Items {
		jobsByName[jobs.Items[i].Name] = &jobs.Items[i]
	}

	// Attach load Jobs to entries. A reservation records the Job name, which
	// stays accurate across version changes. For volumes written by an older
	// version (no ledger) fall back to the historic per-Model Job name.
	for key, res := range view.Reservations {
		entry := view.Entries[key]
		if entry == nil {
			continue
		}
		if job := jobsByName[res.JobName]; job != nil {
			entry.LoadJob = job.Name
		}
	}
	for name, key := range modelEntryKey {
		entry := view.Entries[key]
		if entry == nil || entry.LoadJob != "" {
			continue
		}
		job := jobsByName[loadCacheJobName(&kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: name}})]
		if job == nil {
			continue
		}
		// Never attribute a Job to a different entry. Jobs created by this
		// version carry the entry key; a Job without it belongs to the
		// historic directory convention and is only valid for that directory.
		jobEntry := k8sutils.GetLabel(job, kubeaiv1.CacheJobEntryLabel)
		if jobEntry != "" && jobEntry != key {
			continue
		}
		legacyRelPath := strings.TrimPrefix(modelCacheDir(&kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: name}}), "/")
		entryRelPath := entry.RelPath
		if ledgerEntry := ledger.Entries[key]; ledgerEntry != nil {
			entryRelPath = ledgerEntry.RelPath
		}
		if jobEntry == "" && entryRelPath != "" && entryRelPath != legacyRelPath {
			continue
		}
		entry.LoadJob = job.Name
		switch {
		case k8sutils.IsJobCompleted(job):
			// A completed job means the weights are on the volume even if the
			// ledger update was interrupted.
			entry.Present = true
		case jobHasFailed(job):
			// Leave Present untouched; a failed or interrupted download must
			// never be treated as ready.
		}
	}

	for _, entry := range view.Entries {
		if entry.LoadJob != "" {
			job := jobsByName[entry.LoadJob]
			if jobHasFailed(job) {
				entry.LoadState = cacheLoadStateFailed
				continue
			}
			if !k8sutils.IsJobCompleted(job) {
				entry.LoadState = cacheLoadStateLoading
				continue
			}
		}
		if _, ok := view.Reservations[entry.Key]; ok && !entry.Present {
			entry.LoadState = cacheLoadStatePending
			continue
		}
		if entry.Present {
			entry.LoadState = cacheLoadStateReady
			continue
		}
		if entry.Partial {
			entry.LoadState = cacheLoadStateFailed
			continue
		}
		entry.LoadState = cacheLoadStateUnknown
	}

	return view, nil
}

// jobHasFailed reports whether a Job has terminally failed.
func jobHasFailed(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// podCacheEntryRelPath returns the cache entry directory mounted by a server
// Pod, relative to the volume root. It is derived from the Pod spec, which is
// immutable, so it remains accurate for pods that predate the entry label.
func podCacheEntryRelPath(pod *corev1.Pod, pvcName string) string {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != pvcName {
			continue
		}
		for _, container := range pod.Spec.Containers {
			for _, mount := range container.VolumeMounts {
				if mount.Name != volume.Name {
					continue
				}
				if strings.HasPrefix(mount.MountPath, "/models/") && mount.SubPath != "" {
					return mount.SubPath
				}
			}
		}
	}
	return ""
}

// cacheVolumeBudget returns the capacity declared by the volume. The PVC
// storage request is the declared budget; status capacity is used as a
// fallback when a claim somehow has no request.
func cacheVolumeBudget(pvc *corev1.PersistentVolumeClaim) int64 {
	if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok && !q.IsZero() {
		return q.Value()
	}
	if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok && !q.IsZero() {
		return q.Value()
	}
	return 0
}

// modelCacheExpectedSize reads the declared size hint from the Model.
func modelCacheExpectedSize(model *kubeaiv1.Model) int64 {
	raw := strings.TrimSpace(k8sutils.GetAnnotation(model, kubeaiv1.ModelCacheSizeAnnotation))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// cacheAdmissionDecision is the result of the pre-download admission check.
type cacheAdmissionDecision struct {
	Admitted       bool
	Wait           bool
	Reuse          bool
	Reason         string
	EntryKey       string
	RequiredBytes  int64
	AvailableBytes int64
	ShortfallBytes int64
	Holders        []string
	Reclaim        []string
	RelPath        string
}

// cacheHolders lists the entries that occupy the volume, without exposing
// source URLs or credentials.
func cacheHolders(view *cacheVolumeView) []string {
	var holders []string
	for _, key := range view.sortedEntryKeys() {
		entry := view.Entries[key]
		if !entry.Present || entry.SizeBytes == 0 {
			continue
		}
		who := strings.Join(entry.References, ",")
		if who == "" {
			who = "unreferenced"
		}
		holders = append(holders, fmt.Sprintf("%s=%d bytes (%s)", entry.Key, entry.SizeBytes, who))
	}
	return holders
}

// proposeCacheRelPath assigns a physical path to a new entry. The legacy
// "<name>-<uid>" layout is preserved for the first entry of a Model so
// existing volumes and tooling keep working; a second entry for the same
// object (for example after a version change) gets a key suffix so it can
// never overwrite an entry that is still in use.
func proposeCacheRelPath(model *kubeaiv1.Model, key string, ledger *cacheLedger) string {
	base := strings.TrimPrefix(modelCacheDir(model), "/")
	if !ledger.pathUsed(base) {
		return base
	}
	suffix := strings.TrimPrefix(key, cacheEntryKeyPrefix)
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return fmt.Sprintf("%s-%s", base, suffix)
}

// mutateCacheLedger applies fn to the latest ledger and writes it back with
// optimistic concurrency. A conflict is retried against the fresh object so
// two controllers (or two loads in one controller) cannot both reserve space.
func (r *ModelReconciler) mutateCacheLedger(ctx context.Context, namespace, name string, fn func(*cacheLedger) error) error {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	for attempt := 0; attempt < 5; attempt++ {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, key, pvc); err != nil {
			return err
		}
		ledger := parseCacheLedger(pvc)
		before, err := json.Marshal(ledger)
		if err != nil {
			return err
		}
		if err := fn(ledger); err != nil {
			return err
		}
		after, err := json.Marshal(ledger)
		if err != nil {
			return err
		}
		if bytes.Equal(before, after) {
			return nil
		}
		k8sutils.SetAnnotation(pvc, kubeaiv1.PVCCacheLedgerAnnotation, string(after))
		if err := r.Update(ctx, pvc); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("updating cache ledger: %w", err)
		}
		return nil
	}
	return fmt.Errorf("updating cache ledger: too many conflicts")
}

func appendBounded[T any](items []T, item T, limit int) []T {
	items = append(items, item)
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	return items
}

// cacheVolumeStatus builds the per-volume capacity status shown on Models.
func cacheVolumeStatus(view *cacheVolumeView) *kubeaiv1.ModelCacheVolumeStatus {
	status := &kubeaiv1.ModelCacheVolumeStatus{
		Name:        view.Name,
		BudgetBytes: view.BudgetBytes,
	}
	var used, pending, reclaimable int64
	for _, key := range view.sortedEntryKeys() {
		entry := view.Entries[key]
		if entry.Present {
			used += entry.SizeBytes
		}
		if entry.Reclaimable() {
			reclaimable += entry.SizeBytes
		}
		status.Entries = append(status.Entries, cacheEntryStatus(entry))
	}
	for _, res := range view.Reservations {
		pending += res.Bytes
	}
	status.UsedBytes = used
	status.PendingBytes = pending
	status.ReclaimableBytes = reclaimable
	if view.Ledger != nil {
		status.RecentReclaims = append([]kubeaiv1.ModelCacheReclaimStatus(nil), view.Ledger.RecentReclaims...)
		status.RecentRejections = append([]kubeaiv1.ModelCacheRejectionStatus(nil), view.Ledger.RecentRejections...)
	}
	return status
}

func cacheEntryStatus(entry *cacheEntryView) kubeaiv1.ModelCacheEntryStatus {
	status := kubeaiv1.ModelCacheEntryStatus{
		Key:          entry.Key,
		RelPath:      entry.RelPath,
		SizeBytes:    entry.SizeBytes,
		SizeKnown:    entry.SizeKnown,
		ReferencedBy: entry.referencedByCount(),
		LoadState:    entry.LoadState,
		Reclaimable:  entry.Reclaimable() && len(entry.References) == 0,
	}
	if !entry.LastUsed.IsZero() {
		t := metav1.NewTime(entry.LastUsed)
		status.LastUsed = &t
	}
	return status
}

// resourceListRequests sums the requests of all containers in a Pod.
func resourceListRequests(pod *corev1.Pod) corev1.ResourceList {
	total := corev1.ResourceList{}
	for _, container := range pod.Spec.Containers {
		for name, q := range container.Resources.Requests {
			cur := total[name]
			cur.Add(q)
			total[name] = cur
		}
	}
	return total
}

// quantityOrZero returns the resource quantity or zero when absent.
func quantityOrZero(list corev1.ResourceList, name corev1.ResourceName) resource.Quantity {
	if q, ok := list[name]; ok {
		return q
	}
	return *resource.NewQuantity(0, resource.DecimalSI)
}
