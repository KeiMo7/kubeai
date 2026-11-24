package modelcontroller

import (
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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type PVCModelAnnotationValue struct {
	UID       string    `json:"uid"`
	Timestamp time.Time `json:"timestamp"`
}

// cacheReconcileResult carries the cache conclusion of a reconcile into the
// placement step. It is derived state, never an in-process store of truth.
type cacheReconcileResult struct {
	Key     string
	RelPath string
	Volume  string
	Loaded  bool
	Entry   *cacheEntryView
	View    *cacheVolumeView
}

// reconcileCache ensures the cache entry for a Model is present on its cache
// volume, enforcing capacity before a download job is created.
func (r *ModelReconciler) reconcileCache(ctx context.Context, model *kubeaiv1.Model, cfg ModelConfig) (ctrl.Result, *cacheReconcileResult, error) {
	if model.Status.Cache == nil {
		model.Status.Cache = &kubeaiv1.ModelStatusCache{}
	}
	// Default to not loaded; every successful path below sets it explicitly.
	// This guarantees an interrupted download can never leave a stale true.
	model.Status.Cache.Loaded = false

	modelDeleted := model.DeletionTimestamp != nil
	volumeName := cachePVCName(model, cfg)

	pvc := &corev1.PersistentVolumeClaim{}
	var pvcExists bool
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: model.Namespace, Name: volumeName}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			pvcExists = false
		} else {
			return ctrl.Result{}, nil, fmt.Errorf("getting cache PVC: %w", err)
		}
	} else {
		pvcExists = true
	}

	if !pvcExists {
		if modelDeleted {
			return ctrl.Result{}, nil, nil
		}
		pvc = r.cachePVCForModel(model, cfg)
		if err := r.Create(ctx, pvc); err != nil {
			return ctrl.Result{}, nil, fmt.Errorf("creating cache PVC: %w", err)
		}
	}

	// Caches that are shared across multiple Models require model-specific cleanup.
	if cfg.CacheProfile.SharedFilesystem != nil {
		if controllerutil.AddFinalizer(model, kubeaiv1.ModelCacheEvictionFinalizer) {
			if err := r.Update(ctx, model); err != nil {
				return ctrl.Result{}, nil, fmt.Errorf("adding cache deletion finalizer: %w", err)
			}
		}
	}

	identity := cacheEntryIdentityForModel(model, cfg.Source.url)
	result := &cacheReconcileResult{
		Key:    identity.Key,
		Volume: pvc.Name,
	}

	// Advance any reclaim Job started by an earlier admission decision.
	if err := r.reconcileReclaimJobs(ctx, model.Namespace, pvc); err != nil {
		return ctrl.Result{}, nil, err
	}

	view, err := r.buildCacheView(ctx, pvc, model.Spec.CacheProfile, "")
	if err != nil {
		// Inventory failures must not block the Model. Defer placement and keep
		// the pre-existing behavior of not creating replicas until the cache is
		// known to be ready.
		result.View = nil
		applyCacheStatus(model, nil, result)
		return ctrl.Result{RequeueAfter: cacheRetryDelay}, result, nil
	}
	result.View = view
	entry := view.Entries[identity.Key]
	if entry == nil {
		entry = &cacheEntryView{Key: identity.Key}
	}
	// Adopt an entry discovered from cluster state into the ledger. This keeps
	// volumes written by an older version (and completed Jobs whose ledger
	// update was interrupted) from being downloaded a second time.
	if entry.Present {
		if err := r.adoptPresentEntry(ctx, pvc, model, identity.Key, entry.RelPath); err != nil {
			return ctrl.Result{}, nil, err
		}
	}
	result.Entry = entry
	result.RelPath = entry.RelPath

	// A shared entry that another Model is already downloading must not start
	// a second download and must not be reported ready.
	if !entry.Present {
		if res, ok := view.Reservations[identity.Key]; ok && res.ModelUID != string(model.UID) {
			setAdmissionStatus(model, cacheAdmissionDecision{
				Admitted: false,
				Reason:   "shared cache entry is already being loaded",
				EntryKey: identity.Key,
			}, result.Volume)
			applyCacheStatus(model, result.View, result)
			return ctrl.Result{RequeueAfter: cacheRetryDelay}, result, nil
		}
	}

	jobName := r.loadJobNameForEntry(model, identity.Key, entry, view)
	loadJob := &batchv1.Job{}
	jobExists := true
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: model.Namespace, Name: jobName}, loadJob); err != nil {
		if apierrors.IsNotFound(err) {
			jobExists = false
		} else {
			return ctrl.Result{}, nil, fmt.Errorf("getting cache job: %w", err)
		}
	}

	switch {
	case jobExists && k8sutils.IsJobCompleted(loadJob):
		size, sizeKnown := r.measureCacheEntrySize(ctx, model.Namespace, loadJob.Name)
		if !sizeKnown {
			size, sizeKnown = knownEntrySize(view, identity.Key)
		}
		if err := r.recordCacheLoadComplete(ctx, pvc, model, identity.Key, size, sizeKnown); err != nil {
			return ctrl.Result{}, nil, err
		}
		if err := r.Delete(ctx, loadJob, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil, fmt.Errorf("deleting job: %w", err)
		}
		result.Loaded = true
		result.View = r.refreshCacheView(ctx, pvc, model.Spec.CacheProfile)
		if result.View != nil {
			result.Entry = result.View.Entries[identity.Key]
		}
		result.RelPath = entryRelPath(result.Entry, model)
		applyCacheStatus(model, result.View, result)
		model.Status.Cache.Loaded = true
		return ctrl.Result{}, result, nil
	case jobExists && jobHasFailed(loadJob):
		// A failed or interrupted download neither marks the entry ready nor
		// keeps its reservation, so a retry sees the real free space. Any
		// partial files are removed with the same Job-style cleanup used for a
		// full entry, but only when no other Model references the entry.
		if err := r.recordCacheLoadFailed(ctx, pvc, model, identity.Key, jobName); err != nil {
			return ctrl.Result{}, nil, err
		}
		if err := r.Delete(ctx, loadJob, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil, fmt.Errorf("deleting failed job: %w", err)
		}
		if err := r.cleanupPartialEntry(ctx, model, cfg, pvc, view, identity.Key); err != nil {
			return ctrl.Result{}, nil, err
		}
		applyCacheStatus(model, result.View, result)
		return ctrl.Result{RequeueAfter: cacheRetryDelay}, result, nil
	case jobExists:
		// Download in progress: never report ready, never occupy a replica.
		if err := r.ensureLoadReservation(ctx, pvc, model, identity.Key, jobName); err != nil {
			return ctrl.Result{}, nil, err
		}
		applyCacheStatus(model, result.View, result)
		return ctrl.Result{}, result, nil
	}

	// No load Job. Clean up a stale reservation whose Job disappeared.
	if err := r.pruneStaleReservation(ctx, pvc, identity.Key, string(model.UID)); err != nil {
		return ctrl.Result{}, nil, err
	}

	entry = view.Entries[identity.Key]
	if entry != nil && entry.Present {
		result.Loaded = true
		model.Status.Cache.Loaded = true
		if err := r.ensurePVCModelAnnotation(ctx, pvc, model.Name, string(model.UID)); err != nil {
			return ctrl.Result{}, nil, err
		}
		applyCacheStatus(model, result.View, result)
		return ctrl.Result{}, result, nil
	}
	if entry != nil && entry.Reclaiming {
		applyCacheStatus(model, result.View, result)
		return ctrl.Result{RequeueAfter: cacheRetryDelay}, result, nil
	}

	required, sizeKnown := r.requiredEntrySize(model, view, identity.Key)
	decision, err := r.reserveCacheEntry(ctx, model, pvc, view, identity.Key, required, sizeKnown, jobName)
	if err != nil {
		return ctrl.Result{}, nil, err
	}
	setAdmissionStatus(model, decision, result.Volume)
	// The view is a snapshot; refresh it so the reported usage and pending
	// bytes include the reservation that was just written.
	result.View = r.refreshCacheView(ctx, pvc, model.Spec.CacheProfile)
	if result.View != nil {
		result.Entry = result.View.Entries[identity.Key]
	}
	applyCacheStatus(model, result.View, result)
	if !decision.Admitted {
		// Reclaim jobs are created from the reservation step. Never create a
		// download job when the volume cannot hold the entry.
		if len(decision.Reclaim) > 0 {
			if err := r.createReclaimJobs(ctx, model, cfg, pvc, decision.Reclaim); err != nil {
				return ctrl.Result{}, nil, err
			}
		}
		return ctrl.Result{RequeueAfter: cacheRetryDelay}, result, nil
	}
	if decision.Reuse {
		result.Loaded = true
		model.Status.Cache.Loaded = true
		if err := r.ensurePVCModelAnnotation(ctx, pvc, model.Name, string(model.UID)); err != nil {
			return ctrl.Result{}, nil, err
		}
		return ctrl.Result{}, result, nil
	}

	loadJob = r.loadCacheJobForModel(model, cfg, decision.RelPath)
	if jobName != loadCacheJobName(model) {
		loadJob.Name = jobName
	}
	if loadJob.Labels == nil {
		loadJob.Labels = map[string]string{}
	}
	loadJob.Labels[kubeaiv1.CacheJobEntryLabel] = identity.Key
	if err := ctrl.SetControllerReference(model, loadJob, r.Scheme); err != nil {
		return ctrl.Result{}, nil, fmt.Errorf("setting controller reference on job: %w", err)
	}
	if err := r.Create(ctx, loadJob, k8sutils.DefaultCreateOptions()); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, result, nil
		}
		if cleanupErr := r.removeLoadReservation(ctx, pvc, identity.Key); cleanupErr != nil {
			return ctrl.Result{}, nil, cleanupErr
		}
		return ctrl.Result{}, nil, fmt.Errorf("creating job: %w", err)
	}
	result.RelPath = decision.RelPath
	return ctrl.Result{}, result, nil
}

// loadJobNameForEntry preserves the historic load-cache-<model> Job name for
// the entry that owns the historic directory, and derives a distinct name for
// any other entry (for example after a version change) so an old Job is never
// mistaken for the new entry's download.
func (r *ModelReconciler) loadJobNameForEntry(model *kubeaiv1.Model, key string, entry *cacheEntryView, view *cacheVolumeView) string {
	if view != nil {
		if res, ok := view.Reservations[key]; ok && res.JobName != "" {
			return res.JobName
		}
	}
	relPath := ""
	if entry != nil {
		relPath = entry.RelPath
	}
	if relPath == "" && view != nil && view.Ledger != nil {
		relPath = proposeCacheRelPath(model, key, view.Ledger)
	}
	legacy := strings.TrimPrefix(modelCacheDir(model), "/")
	if relPath == "" || relPath == legacy {
		return loadCacheJobName(model)
	}
	suffix := strings.TrimPrefix(key, cacheEntryKeyPrefix)
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return fmt.Sprintf("%s-%s", loadCacheJobName(model), suffix)
}

// adoptPresentEntry records an entry that is already on the volume even though
// the ledger does not know it yet.
func (r *ModelReconciler) adoptPresentEntry(ctx context.Context, pvc *corev1.PersistentVolumeClaim, model *kubeaiv1.Model, key, relPath string) error {
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		entry := l.Entries[key]
		if entry == nil {
			if relPath == "" {
				relPath = proposeCacheRelPath(model, key, l)
			}
			// Adopted entries are not measured; the size is calibrated from
			// the load Job or a later reconcile. Their size is left unknown
			// rather than guessed so it never causes a false rejection.
			l.Entries[key] = &cacheLedgerEntry{RelPath: relPath, Present: true}
			return nil
		}
		if !entry.Present {
			entry.Present = true
		}
		if entry.RelPath == "" && relPath != "" {
			entry.RelPath = relPath
		}
		return nil
	})
}

// refreshCacheView re-reads the volume after a ledger mutation so status is
// derived from the same state that actions operate on.
func (r *ModelReconciler) refreshCacheView(ctx context.Context, pvc *corev1.PersistentVolumeClaim, cacheProfile string) *cacheVolumeView {
	latest := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), latest); err != nil {
		return nil
	}
	view, err := r.buildCacheView(ctx, latest, cacheProfile, "")
	if err != nil {
		return nil
	}
	return view
}

// applyCacheStatus populates per-entry and per-volume status for the Model.
func applyCacheStatus(model *kubeaiv1.Model, view *cacheVolumeView, result *cacheReconcileResult) {
	if model.Status.Cache == nil {
		model.Status.Cache = &kubeaiv1.ModelStatusCache{}
	}
	if result == nil {
		return
	}
	if result.Entry != nil {
		status := cacheEntryStatus(result.Entry)
		status.Volume = result.Volume
		model.Status.Cache.Entry = &status
	}
	if view != nil {
		model.Status.Cache.Volume = cacheVolumeStatus(view)
	}
}

// reconcileReclaimJobs advances reclaim jobs and frees budget when they finish.
func (r *ModelReconciler) reconcileReclaimJobs(ctx context.Context, namespace string, pvc *corev1.PersistentVolumeClaim) error {
	ledger := parseCacheLedger(pvc)
	for key, entry := range ledger.Entries {
		if entry == nil || !entry.Reclaiming {
			continue
		}
		job := &batchv1.Job{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: reclaimCacheJobName(key)}, job)
		switch {
		case err == nil:
			if jobHasFailed(job) {
				// The entry is still on disk: clear the flag so the next
				// admission can retry, and keep the size accounted for.
				if err := r.clearReclaiming(ctx, pvc, key, "cache reclaim job failed"); err != nil {
					return err
				}
				if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("deleting failed reclaim job: %w", err)
				}
				continue
			}
			if !k8sutils.IsJobCompleted(job) {
				continue
			}
		case apierrors.IsNotFound(err):
			// A missing Job right after the entry was marked must not be
			// treated as success; the Job may simply not be visible yet.
			// After the grace period it is treated as complete so the entry
			// cannot hold budget forever.
			if entry.ReclaimingSince == nil || time.Since(*entry.ReclaimingSince) < cacheReclaimMissingJobGrace {
				continue
			}
		default:
			return fmt.Errorf("getting reclaim job: %w", err)
		}
		if err := r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
			e := l.Entries[key]
			if e == nil || !e.Reclaiming {
				return nil
			}
			reason := e.ReclaimReason
			if reason == "" {
				reason = "reclaimed capacity for a new cache entry"
			}
			l.RecentReclaims = appendBounded(l.RecentReclaims, kubeaiv1.ModelCacheReclaimStatus{
				EntryKey:  key,
				SizeBytes: e.SizeBytes,
				Reason:    reason,
				At:        metav1.Now(),
			}, cacheHistoryLimit)
			delete(l.Entries, key)
			delete(l.Reservations, key)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// clearReclaiming releases a reclaim marker without deleting the entry.
func (r *ModelReconciler) clearReclaiming(ctx context.Context, pvc *corev1.PersistentVolumeClaim, key, reason string) error {
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		e := l.Entries[key]
		if e == nil || !e.Reclaiming {
			return nil
		}
		e.Reclaiming = false
		e.ReclaimingSince = nil
		e.ReclaimReason = ""
		now := time.Now()
		e.ReclaimAttempted = &now
		l.RecentRejections = appendBounded(l.RecentRejections, kubeaiv1.ModelCacheRejectionStatus{
			EntryKey: key,
			Reason:   reason,
			At:       metav1.Now(),
		}, cacheHistoryLimit)
		return nil
	})
}

func reclaimCacheJobName(key string) string {
	return fmt.Sprintf("reclaim-cache-%s", key)
}

func (r *ModelReconciler) createReclaimJobs(ctx context.Context, model *kubeaiv1.Model, cfg ModelConfig, pvc *corev1.PersistentVolumeClaim, keys []string) error {
	ledger := parseCacheLedger(pvc)
	for _, key := range keys {
		entry := ledger.Entries[key]
		if entry == nil {
			continue
		}
		// Reclaim is a volume-level operation: the Job is deliberately not
		// owned by the requesting Model so deleting that Model cannot abort a
		// reclaim half-way. It carries a short TTL and is tracked by the
		// ledger instead of by an owner reference.
		job := r.reclaimCacheJobForModel(model, cfg, key, entry.RelPath)
		if err := r.Create(ctx, job, k8sutils.DefaultCreateOptions()); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating reclaim job: %w", err)
		}
	}
	return nil
}

// reserveCacheEntry atomically checks capacity and writes a reservation. The
// check and the write happen under optimistic concurrency on the volume
// annotation, so two concurrent loads can never both think they fit.
func (r *ModelReconciler) reserveCacheEntry(ctx context.Context, model *kubeaiv1.Model, pvc *corev1.PersistentVolumeClaim, view *cacheVolumeView, key string, required int64, sizeKnown bool, jobName string) (cacheAdmissionDecision, error) {
	reclaimable := map[string]bool{}
	for _, entry := range view.Entries {
		if entry.Reclaimable() {
			reclaimable[entry.Key] = true
		}
	}

	var decision cacheAdmissionDecision
	err := r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		decision = computeLedgerAdmission(l, view, key, required, sizeKnown, string(model.UID), reclaimable)
		switch {
		case decision.Wait:
			now := time.Now()
			for _, reclaimKey := range decision.Reclaim {
				if entry := l.Entries[reclaimKey]; entry != nil {
					entry.Reclaiming = true
					if entry.ReclaimingSince == nil {
						entry.ReclaimingSince = &now
					}
					entry.ReclaimReason = "reclaimed capacity for a new cache entry"
				}
			}
		case decision.Admitted && !decision.Reuse:
			if decision.RelPath == "" {
				decision.RelPath = proposeCacheRelPath(model, key, l)
			}
			l.Entries[key] = &cacheLedgerEntry{
				RelPath:   decision.RelPath,
				SizeBytes: required,
				SizeKnown: sizeKnown,
			}
			l.Reservations[key] = cacheLedgerReservation{
				EntryKey:  key,
				ModelName: model.Name,
				ModelUID:  string(model.UID),
				JobName:   jobName,
				Bytes:     required,
				SizeKnown: sizeKnown,
				StartedAt: time.Now(),
			}
		case !decision.Admitted:
			l.RecentRejections = appendBounded(l.RecentRejections, kubeaiv1.ModelCacheRejectionStatus{
				EntryKey:       key,
				RequiredBytes:  decision.RequiredBytes,
				AvailableBytes: decision.AvailableBytes,
				ShortfallBytes: decision.ShortfallBytes,
				Reason:         decision.Reason,
				At:             metav1.Now(),
			}, cacheHistoryLimit)
		}
		return nil
	})
	if err != nil {
		return cacheAdmissionDecision{}, err
	}
	if len(decision.Reclaim) > 0 {
		decision.Reclaim = filterReclaimable(view, decision.Reclaim)
	}
	return decision, nil
}

// computeLedgerAdmission recomputes admission from the freshest ledger. Entry
// sizes and reservations come from the ledger itself; the view supplies the
// reclaimable set so an in-use entry is never selected.
func computeLedgerAdmission(l *cacheLedger, view *cacheVolumeView, key string, required int64, sizeKnown bool, modelUID string, reclaimable map[string]bool) cacheAdmissionDecision {
	d := cacheAdmissionDecision{EntryKey: key, RequiredBytes: required}

	if e := l.Entries[key]; e != nil && e.Present {
		d.Admitted = true
		d.Reuse = true
		d.RelPath = e.RelPath
		d.Reason = "cache entry already present"
		return d
	}
	if res, ok := l.Reservations[key]; ok {
		if res.ModelUID == modelUID {
			d.Admitted = true
			d.Reason = "load already in progress for this model"
			return d
		}
		d.Wait = true
		d.Reason = "shared cache entry is already being loaded"
		return d
	}

	budget := cacheVolumeBudgetFromView(view)
	if budget <= 0 {
		d.Admitted = true
		d.Reason = "volume has no declared capacity"
		if e := l.Entries[key]; e != nil {
			d.RelPath = e.RelPath
		}
		return d
	}

	var used, pending int64
	for _, e := range l.Entries {
		if e != nil && e.Present {
			used += e.SizeBytes
		}
	}
	for k, res := range l.Reservations {
		if k == key {
			continue
		}
		pending += res.Bytes
	}
	available := budget - used - pending
	if available < 0 {
		available = 0
	}
	d.AvailableBytes = available

	if !sizeKnown && required == 0 {
		d.Admitted = true
		d.Reason = "entry size unknown; admitted and will be calibrated after load"
		if e := l.Entries[key]; e != nil {
			d.RelPath = e.RelPath
		}
		return d
	}
	if required <= available {
		d.Admitted = true
		d.Reason = fmt.Sprintf("fits in %d available bytes", available)
		if e := l.Entries[key]; e != nil {
			d.RelPath = e.RelPath
		}
		return d
	}

	var candidates []*cacheEntryView
	var reclaiming int
	for _, entry := range view.Entries {
		if entry.Reclaiming {
			reclaiming++
		}
		if entry.Key == key || !reclaimable[entry.Key] {
			continue
		}
		ledgerEntry := l.Entries[entry.Key]
		if ledgerEntry == nil || ledgerEntry.Reclaiming || !ledgerEntry.SizeKnown || ledgerEntry.SizeBytes <= 0 {
			// An entry with an unknown or zero size cannot be trusted to free
			// enough space; reclaiming it would not make progress.
			continue
		}
		if ledgerEntry.ReclaimAttempted != nil && time.Since(*ledgerEntry.ReclaimAttempted) < cacheReclaimBackoff {
			// A recent reclaim failed; do not spin on it every reconcile.
			continue
		}
		candidates = append(candidates, entry)
	}
	if reclaiming > 0 && len(candidates) == 0 {
		// A reclaim is already in flight; this is progress, not a rejection.
		d.Wait = true
		d.Reason = "waiting for an in-flight capacity reclaim to complete"
		return d
	}
	// Least recently used first; the key breaks ties deterministically.
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].LastUsed.Equal(candidates[j].LastUsed) {
			return candidates[i].LastUsed.Before(candidates[j].LastUsed)
		}
		return candidates[i].Key < candidates[j].Key
	})

	var reclaimed int64
	var selected []string
	for _, entry := range candidates {
		if required <= available+reclaimed {
			break
		}
		reclaimed += entry.SizeBytes
		selected = append(selected, entry.Key)
	}
	if required <= available+reclaimed {
		d.Wait = true
		d.Reclaim = selected
		d.Reason = fmt.Sprintf("need to reclaim %d entry(s) before loading", len(selected))
		return d
	}

	d.ShortfallBytes = required - (available + reclaimed)
	d.Holders = cacheHolders(view)
	d.Reason = fmt.Sprintf("insufficient cache capacity: need %d more bytes", d.ShortfallBytes)
	return d
}

func filterReclaimable(view *cacheVolumeView, keys []string) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if entry := view.Entries[key]; entry != nil && entry.Reclaimable() {
			out = append(out, key)
		}
	}
	return out
}

func cacheVolumeBudgetFromView(view *cacheVolumeView) int64 {
	if view == nil || !view.BudgetKnown {
		return 0
	}
	return view.BudgetBytes
}

func setAdmissionStatus(model *kubeaiv1.Model, d cacheAdmissionDecision, volume string) {
	if model.Status.Cache == nil {
		model.Status.Cache = &kubeaiv1.ModelStatusCache{}
	}
	status := &kubeaiv1.ModelCacheAdmissionStatus{
		Admitted:       d.Admitted,
		Reason:         d.Reason,
		Volume:         volume,
		RequiredBytes:  d.RequiredBytes,
		AvailableBytes: d.AvailableBytes,
		ShortfallBytes: d.ShortfallBytes,
		Holders:        d.Holders,
		Reclaimed:      d.Reclaim,
	}
	now := metav1.Now()
	status.At = &now
	model.Status.Cache.Admission = status
}

func (r *ModelReconciler) requiredEntrySize(model *kubeaiv1.Model, view *cacheVolumeView, key string) (int64, bool) {
	if view != nil {
		if entry := view.Entries[key]; entry != nil && entry.SizeKnown && entry.SizeBytes > 0 {
			return entry.SizeBytes, true
		}
	}
	if n := modelCacheExpectedSize(model); n > 0 {
		return n, true
	}
	return 0, false
}

func knownEntrySize(view *cacheVolumeView, key string) (int64, bool) {
	if view == nil {
		return 0, false
	}
	if entry := view.Entries[key]; entry != nil && entry.SizeKnown {
		return entry.SizeBytes, entry.SizeBytes > 0
	}
	return 0, false
}

// ensureLoadReservation keeps an in-flight load accounted for on the volume.
func (r *ModelReconciler) ensureLoadReservation(ctx context.Context, pvc *corev1.PersistentVolumeClaim, model *kubeaiv1.Model, key, jobName string) error {
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		if _, ok := l.Reservations[key]; ok {
			return nil
		}
		if e := l.Entries[key]; e != nil && e.Present {
			return nil
		}
		entry := l.Entries[key]
		if entry == nil {
			entry = &cacheLedgerEntry{RelPath: proposeCacheRelPath(model, key, l)}
			l.Entries[key] = entry
		}
		l.Reservations[key] = cacheLedgerReservation{
			EntryKey:  key,
			ModelName: model.Name,
			ModelUID:  string(model.UID),
			JobName:   jobName,
			Bytes:     entry.SizeBytes,
			SizeKnown: entry.SizeKnown,
			StartedAt: time.Now(),
		}
		return nil
	})
}

func (r *ModelReconciler) removeLoadReservation(ctx context.Context, pvc *corev1.PersistentVolumeClaim, key string) error {
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		delete(l.Reservations, key)
		if e := l.Entries[key]; e != nil && !e.Present && !e.Reclaiming {
			delete(l.Entries, key)
		}
		return nil
	})
}

func (r *ModelReconciler) pruneStaleReservation(ctx context.Context, pvc *corev1.PersistentVolumeClaim, key, modelUID string) error {
	ledger := parseCacheLedger(pvc)
	res, ok := ledger.Reservations[key]
	if !ok || res.ModelUID != modelUID || res.JobName == "" {
		return nil
	}
	job := &batchv1.Job{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: res.JobName}, job); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return nil
	}
	// Give a just-created Job a grace period to become visible.
	if time.Since(res.StartedAt) < time.Minute {
		return nil
	}
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		if cur, ok := l.Reservations[key]; ok && cur.JobName == res.JobName {
			delete(l.Reservations, key)
		}
		if e := l.Entries[key]; e != nil && !e.Present && !e.Reclaiming {
			delete(l.Entries, key)
		}
		return nil
	})
}

// recordCacheLoadComplete marks the entry present and calibrated.
func (r *ModelReconciler) recordCacheLoadComplete(ctx context.Context, pvc *corev1.PersistentVolumeClaim, model *kubeaiv1.Model, key string, size int64, sizeKnown bool) error {
	now := time.Now()
	if err := r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		entry := l.Entries[key]
		if entry == nil {
			entry = &cacheLedgerEntry{RelPath: proposeCacheRelPath(model, key, l)}
			l.Entries[key] = entry
		}
		entry.Present = true
		entry.Reclaiming = false
		entry.Partial = false
		if sizeKnown && size > 0 {
			entry.SizeBytes = size
			entry.SizeKnown = true
		}
		entry.LastUsed = &now
		delete(l.Reservations, key)
		return nil
	}); err != nil {
		return err
	}
	return r.updatePVCModelAnnotation(ctx, pvc.Namespace, pvc.Name, model.Name, PVCModelAnnotationValue{
		UID:       string(model.UID),
		Timestamp: now,
	})
}

// recordCacheLoadFailed clears the reservation so a retry sees the real free
// space and marks the directory partial. A partial entry is never ready, never
// reclaimable for capacity and never counted as used.
func (r *ModelReconciler) recordCacheLoadFailed(ctx context.Context, pvc *corev1.PersistentVolumeClaim, model *kubeaiv1.Model, key, jobName string) error {
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		if res, ok := l.Reservations[key]; ok && (res.JobName == jobName || res.ModelUID == string(model.UID)) {
			delete(l.Reservations, key)
		}
		if entry := l.Entries[key]; entry != nil && !entry.Present && !entry.Reclaiming {
			entry.Partial = true
			entry.ReclaimingSince = nil
		}
		return nil
	})
}

// measureCacheEntrySize reads the calibrated size from the load Job's
// termination log. Unreadable or malformed output simply leaves the size
// unknown; it never fails the reconcile.
func (r *ModelReconciler) measureCacheEntrySize(ctx context.Context, namespace, jobName string) (int64, bool) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{batchv1.JobNameLabel: jobName}); err != nil {
		return 0, false
	}
	for i := range pods.Items {
		for _, status := range pods.Items[i].Status.ContainerStatuses {
			if status.State.Terminated == nil {
				continue
			}
			if n, ok := parseSizeOutput(status.State.Terminated.Message); ok {
				return n, true
			}
		}
	}
	return 0, false
}

func parseSizeOutput(message string) (int64, bool) {
	fields := strings.Fields(strings.TrimSpace(message))
	if len(fields) == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// finalizeCache releases this Model's cache reference. A shared entry that is
// still referenced or served by another Model is not evicted.
func (r *ModelReconciler) finalizeCache(ctx context.Context, model *kubeaiv1.Model, cfg ModelConfig) error {
	pvc := &corev1.PersistentVolumeClaim{}
	var pvcExists bool
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: model.Namespace, Name: cachePVCName(model, cfg)}, pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("getting cache PVC: %w", err)
		}
	} else {
		pvcExists = true
	}

	if !pvcExists || pvc.DeletionTimestamp != nil {
		if err := r.deleteAllCacheJobsAndPods(ctx, model); err != nil {
			return fmt.Errorf("deleting all cache jobs and pods: %w", err)
		}
		if controllerutil.RemoveFinalizer(model, kubeaiv1.ModelCacheEvictionFinalizer) {
			if err := r.Update(ctx, model); err != nil {
				return fmt.Errorf("removing cache deletion finalizer: %w", err)
			}
		}
		return nil
	}

	if controllerutil.ContainsFinalizer(model, kubeaiv1.ModelCacheEvictionFinalizer) {
		identity := cacheEntryIdentityForModel(model, cfg.Source.url)
		view, err := r.buildCacheView(ctx, pvc, model.Spec.CacheProfile, model.Name)
		if err != nil {
			return fmt.Errorf("building cache view: %w", err)
		}
		entry := view.Entries[identity.Key]
		_, reserved := view.Reservations[identity.Key]
		shared := entry != nil && (len(entry.References) > 0 || entry.ServingPods > 0 || entry.LoadJob != "")
		// The excluded Model's own in-flight load must still protect the
		// entry: deleting half-downloaded weights or racing the loader would
		// leave a corrupt cache for any other Model that shares the source.
		if reserved {
			shared = true
		}
		if !shared && r.modelHasActiveCacheJob(ctx, model) {
			shared = true
		}

		if !shared {
			if err := r.runModelEvictJob(ctx, model, cfg, pvc, identity.Key, entry); err != nil {
				return err
			}
		}

		if err := r.removePVCModelAnnotation(ctx, model.Namespace, pvc.Name, model.Name); err != nil {
			return err
		}
		controllerutil.RemoveFinalizer(model, kubeaiv1.ModelCacheEvictionFinalizer)
		if err := r.Update(ctx, model); err != nil {
			return fmt.Errorf("removing cache deletion finalizer: %w", err)
		}
	}

	if err := r.deleteAllCacheJobsAndPods(ctx, model); err != nil {
		return fmt.Errorf("deleting all cache jobs and pods: %w", err)
	}
	return nil
}

// cleanupPartialEntry removes the directory of a failed download when nothing
// else can be using it. A half-downloaded directory must neither pass as ready
// nor accumulate on the volume; when another Model shares the entry it is left
// for the next successful retry to overwrite.
func (r *ModelReconciler) cleanupPartialEntry(ctx context.Context, model *kubeaiv1.Model, cfg ModelConfig, pvc *corev1.PersistentVolumeClaim, view *cacheVolumeView, key string) error {
	entry := view.Entries[key]
	if entry == nil {
		return nil
	}
	if entry.ServingPods > 0 {
		return nil
	}
	// References include the current Model; anything beyond it means another
	// Model shares this entry.
	for _, ref := range entry.References {
		if ref != model.Name {
			return nil
		}
	}
	if entry.RelPath == "" {
		return nil
	}
	job := r.reclaimCacheJobForModel(model, cfg, key, entry.RelPath)
	// A distinct name keeps the cleanup of an interrupted download separate
	// from a capacity reclaim, which is tracked in the ledger.
	job.Name = fmt.Sprintf("cleanup-cache-%s", strings.TrimPrefix(key, cacheEntryKeyPrefix))
	job.Labels[kubeaiv1.CacheJobRoleLabel] = "cleanup"
	if err := r.Create(ctx, job, k8sutils.DefaultCreateOptions()); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating partial cleanup job: %w", err)
	}
	return nil
}

// modelHasActiveCacheJob reports whether any cache Job for this Model is still
// running. It protects an entry during Model deletion when the Model no longer
// appears in the computed view.
func (r *ModelReconciler) modelHasActiveCacheJob(ctx context.Context, model *kubeaiv1.Model) bool {
	jobs := &batchv1.JobList{}
	if err := r.List(ctx, jobs, client.InNamespace(model.Namespace), client.MatchingLabels{
		kubeaiv1.CacheJobModelLabel: model.Name,
	}); err != nil {
		// On an inventory error, be conservative and do not evict.
		return true
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if k8sutils.GetLabel(job, kubeaiv1.CacheJobRoleLabel) != "load" {
			continue
		}
		if job.DeletionTimestamp != nil {
			continue
		}
		if !k8sutils.IsJobCompleted(job) && !jobHasFailed(job) {
			return true
		}
	}
	return false
}

// runModelEvictJob keeps the historic evict-cache-<model> Job for the
// unreferenced entry, and removes it from the ledger on completion.
func (r *ModelReconciler) runModelEvictJob(ctx context.Context, model *kubeaiv1.Model, cfg ModelConfig, pvc *corev1.PersistentVolumeClaim, key string, entry *cacheEntryView) error {
	evictJob := &batchv1.Job{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: model.Namespace, Name: evictCacheJobName(model)}, evictJob)
	if apierrors.IsNotFound(err) {
		job := r.evictCacheJobForModel(model, cfg, entryRelPath(entry, model))
		if err := ctrl.SetControllerReference(model, job, r.Scheme); err != nil {
			return fmt.Errorf("setting controller reference on cache deletion job: %w", err)
		}
		if err := r.Create(ctx, job, k8sutils.DefaultCreateOptions()); err != nil {
			return fmt.Errorf("creating cache deletion job: %w", err)
		}
		return errReturnEarly
	}
	if err != nil {
		return fmt.Errorf("getting cache deletion job: %w", err)
	}
	if !k8sutils.IsJobCompleted(evictJob) {
		return errReturnEarly
	}
	return r.mutateCacheLedger(ctx, pvc.Namespace, pvc.Name, func(l *cacheLedger) error {
		delete(l.Reservations, key)
		delete(l.Entries, key)
		return nil
	})
}

func entryRelPath(entry *cacheEntryView, model *kubeaiv1.Model) string {
	if entry != nil && entry.RelPath != "" {
		return entry.RelPath
	}
	return strings.TrimPrefix(modelCacheDir(model), "/")
}

func (r *ModelReconciler) deleteAllCacheJobsAndPods(ctx context.Context, model *kubeaiv1.Model) error {
	jobNames := []string{
		loadCacheJobName(model),
		evictCacheJobName(model),
	}

	for _, jobName := range jobNames {
		if err := r.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: model.Namespace, Name: jobName}}); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("deleting job %q: %w", jobName, err)
			}
		}
		if err := r.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace(model.Namespace), client.MatchingLabels{
			batchv1.JobNameLabel: jobName,
		}); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("deleting pods for job %q: %w", jobName, err)
			}
		}
	}

	// Cache Jobs for superseded entries have entry-specific names. Remove them
	// by owner label so deleting a Model cannot leak load/evict/reclaim Jobs.
	if err := r.DeleteAllOf(ctx, &batchv1.Job{}, client.InNamespace(model.Namespace), client.MatchingLabels{
		kubeaiv1.CacheJobModelLabel: model.Name,
	}); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting cache jobs by label: %w", err)
		}
	}
	return nil
}

func parsePVCModelAnnotation(pvc *corev1.PersistentVolumeClaim, modelName string) (PVCModelAnnotationValue, error) {
	pvcModelStatusJSON := k8sutils.GetAnnotation(pvc, kubeaiv1.PVCModelAnnotation(modelName))
	if pvcModelStatusJSON == "" {
		return PVCModelAnnotationValue{}, nil
	}
	var status PVCModelAnnotationValue
	if err := json.Unmarshal([]byte(pvcModelStatusJSON), &status); err != nil {
		return PVCModelAnnotationValue{}, fmt.Errorf("unmarshalling pvc model status: %w", err)
	}
	return status, nil
}

// ensurePVCModelAnnotation writes the legacy per-Model annotation only when it
// is missing or stale. Rewriting it on every reconcile would needlessly change
// cluster state.
func (r *ModelReconciler) ensurePVCModelAnnotation(ctx context.Context, pvc *corev1.PersistentVolumeClaim, modelName, uid string) error {
	ann, err := parsePVCModelAnnotation(pvc, modelName)
	if err != nil {
		// A corrupt legacy annotation is rewritten with the authoritative UID.
		ann = PVCModelAnnotationValue{}
	}
	if ann.UID == uid {
		return nil
	}
	return r.updatePVCModelAnnotation(ctx, pvc.Namespace, pvc.Name, modelName, PVCModelAnnotationValue{
		UID:       uid,
		Timestamp: time.Now(),
	})
}

// updatePVCModelAnnotation writes the legacy per-Model annotation with a fresh
// object so it cannot conflict with a concurrent ledger update.
func (r *ModelReconciler) updatePVCModelAnnotation(ctx context.Context, namespace, name, modelName string, status PVCModelAnnotationValue) error {
	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("marshalling pvc model status: %w", err)
	}
	return retryOnConflict(func() error {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pvc); err != nil {
			return err
		}
		k8sutils.SetAnnotation(pvc, kubeaiv1.PVCModelAnnotation(modelName), string(statusJSON))
		return r.Client.Update(ctx, pvc)
	})
}

func (r *ModelReconciler) removePVCModelAnnotation(ctx context.Context, namespace, name, modelName string) error {
	return retryOnConflict(func() error {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if pvc.Annotations == nil {
			return nil
		}
		if _, ok := pvc.Annotations[kubeaiv1.PVCModelAnnotation(modelName)]; !ok {
			return nil
		}
		delete(pvc.Annotations, kubeaiv1.PVCModelAnnotation(modelName))
		return r.Client.Update(ctx, pvc)
	})
}

func retryOnConflict(fn func() error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = fn()
		if err == nil || !apierrors.IsConflict(err) {
			return err
		}
	}
	return err
}

func (r *ModelReconciler) cachePVCForModel(m *kubeaiv1.Model, c ModelConfig) *corev1.PersistentVolumeClaim {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cachePVCName(m, c),
			Namespace: m.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{},
	}
	switch {
	case c.CacheProfile.SharedFilesystem != nil:
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		storageClassName := c.CacheProfile.SharedFilesystem.StorageClassName
		pvc.Spec.StorageClassName = &storageClassName
		pvc.Spec.VolumeName = c.CacheProfile.SharedFilesystem.PersistentVolumeName
		pvc.Spec.Resources.Requests = corev1.ResourceList{
			// https://discuss.huggingface.co/t/how-to-get-model-size/11038/7
			corev1.ResourceStorage: resource.MustParse(c.CacheProfile.SharedFilesystem.StorageSize),
		}
	default:
		panic("unsupported cache profile, this point should not be reached")
	}
	return &pvc
}

func cachePVCName(m *kubeaiv1.Model, c ModelConfig) string {
	switch {
	case c.CacheProfile.SharedFilesystem != nil:
		// One PVC for all models.
		return fmt.Sprintf("shared-model-cache-%s", m.Spec.CacheProfile)
	default:
		// One PVC per model.
		return fmt.Sprintf("model-cache-%s-%s", m.Name, m.UID[0:7])
	}
}

func (r *ModelReconciler) loadCacheJobForModel(m *kubeaiv1.Model, c ModelConfig, relPath string) *batchv1.Job {
	var env []corev1.EnvVar
	var envKeys []string
	for key := range m.Spec.Env {
		envKeys = append(envKeys, key)
	}
	sort.Strings(envKeys)
	for _, key := range envKeys {
		env = append(env, corev1.EnvVar{
			Name:  key,
			Value: m.Spec.Env[key],
		})
	}
	if relPath == "" {
		relPath = strings.TrimPrefix(modelCacheDir(m), "/")
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      loadCacheJobName(m),
			Namespace: m.Namespace,
			Labels: map[string]string{
				kubeaiv1.CacheJobModelLabel: m.Name,
				kubeaiv1.CacheJobRoleLabel:  "load",
				kubeaiv1.CacheJobEntryLabel: c.CacheEntryKey,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](60),
			Parallelism:             ptr.To[int32](1),
			Completions:             ptr.To[int32](1),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyOnFailure,
					SecurityContext: r.ModelServerPods.ModelPodSecurityContext,
					Containers: []corev1.Container{
						{
							Name:            "loader",
							Env:             env,
							SecurityContext: r.ModelServerPods.ModelContainerSecurityContext,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "model",
									MountPath: modelCacheDir(m),
									SubPath:   relPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "model",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: cachePVCName(m, c),
								},
							},
						},
					},
				},
			},
		},
	}

	job.Spec.Template.Spec.Containers[0].Image = r.ModelLoaders.Image
	// The loader entrypoint still performs the download; the wrapper records
	// the measured size in the termination message so the ledger can be
	// calibrated without a second Job. "du -sk" is used because the loader
	// image ships busybox, whose du has no -b, and a measurement failure must
	// never fail an otherwise successful download.
	job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c"}
	job.Spec.Template.Spec.Containers[0].Args = []string{
		`/bin/load "$1" "$2"; rc=$?; if [ "$rc" -eq 0 ]; then du -sk "$2" 2>/dev/null | awk '{print $1 * 1024}' > /dev/termination-log || true; fi; exit "$rc"`,
		"load",
		m.Spec.URL,
		modelCacheDir(m),
	}
	c.Source.modelSourcePodAdditions.applyToPodSpec(&job.Spec.Template.Spec, 0)

	return job
}

func (r *ModelReconciler) evictCacheJobForModel(m *kubeaiv1.Model, c ModelConfig, relPath string) *batchv1.Job {
	if relPath == "" {
		relPath = strings.TrimPrefix(modelCacheDir(m), "/")
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      evictCacheJobName(m),
			Namespace: m.Namespace,
			Labels: map[string]string{
				kubeaiv1.CacheJobModelLabel: m.Name,
				kubeaiv1.CacheJobRoleLabel:  "evict",
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](60),
			Parallelism:             ptr.To[int32](1),
			Completions:             ptr.To[int32](1),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyOnFailure,
					SecurityContext: r.ModelServerPods.ModelPodSecurityContext,
					Containers: []corev1.Container{
						{
							Name:            "evictor",
							SecurityContext: r.ModelServerPods.ModelContainerSecurityContext,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "model",
									MountPath: "/models",
									SubPath:   "models",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "model",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: cachePVCName(m, c),
								},
							},
						},
					},
				},
			},
		},
	}

	job.Spec.Template.Spec.Containers[0].Image = r.ModelLoaders.Image
	job.Spec.Template.Spec.Containers[0].Command = []string{"bash", "-c", "rm -rf " + cacheEntryVolumePathForRelPath(relPath)}

	return job
}

func (r *ModelReconciler) reclaimCacheJobForModel(m *kubeaiv1.Model, c ModelConfig, key, relPath string) *batchv1.Job {
	if relPath == "" {
		relPath = strings.TrimPrefix(modelCacheDir(m), "/")
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reclaimCacheJobName(key),
			Namespace: m.Namespace,
			Labels: map[string]string{
				kubeaiv1.CacheJobRoleLabel:  "reclaim",
				kubeaiv1.CacheJobEntryLabel: key,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To[int32](60),
			Parallelism:             ptr.To[int32](1),
			Completions:             ptr.To[int32](1),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyOnFailure,
					SecurityContext: r.ModelServerPods.ModelPodSecurityContext,
					Containers: []corev1.Container{
						{
							Name:            "reclaimer",
							SecurityContext: r.ModelServerPods.ModelContainerSecurityContext,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "model",
									MountPath: "/models",
									SubPath:   "models",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "model",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: cachePVCName(m, c),
								},
							},
						},
					},
				},
			},
		},
	}

	job.Spec.Template.Spec.Containers[0].Image = r.ModelLoaders.Image
	job.Spec.Template.Spec.Containers[0].Command = []string{"bash", "-c", "rm -rf " + cacheEntryVolumePathForRelPath(relPath)}
	return job
}

func cacheEntryVolumePathForRelPath(relPath string) string {
	return "/" + strings.TrimLeft(relPath, "/")
}

func modelCacheDir(m *kubeaiv1.Model) string {
	return fmt.Sprintf("/models/%s-%s", m.Name, m.UID)
}

func loadCacheJobName(m *kubeaiv1.Model) string {
	return fmt.Sprintf("load-cache-%s", m.Name)
}

func evictCacheJobName(m *kubeaiv1.Model) string {
	return fmt.Sprintf("evict-cache-%s", m.Name)
}

// updateServingPodCacheLabels labels server Pods with the cache entry they
// actually mount. It only touches labels, never the Pod spec, so a Pod is not
// recreated. Pods still serving the previous entry during a rollout keep that
// entry protected and are not mislabeled with the new one.
func (r *ModelReconciler) updateServingPodCacheLabels(ctx context.Context, pods []*corev1.Pod, key, relPath, pvcName string) error {
	if key == "" || relPath == "" {
		return nil
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if podCacheEntryRelPath(pod, pvcName) != relPath {
			continue
		}
		if k8sutils.GetLabel(pod, kubeaiv1.PodCacheEntryLabel) == key {
			continue
		}
		if err := r.updatePodAddLabel(ctx, pod, kubeaiv1.PodCacheEntryLabel, key); err != nil {
			return err
		}
	}
	return nil
}

func patchServerCacheVolumes(podSpec *corev1.PodSpec, m *kubeaiv1.Model, c ModelConfig) {
	if m.Spec.CacheProfile == "" {
		return
	}
	relPath := strings.TrimPrefix(modelCacheDir(m), "/")
	if c.CacheEntryRelPath != "" {
		relPath = c.CacheEntryRelPath
	}
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: "models",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: cachePVCName(m, c),
			},
		},
	})
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == "server" {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      "models",
				MountPath: modelCacheDir(m),
				SubPath:   relPath,
				ReadOnly:  true,
			})
		}
	}
}
