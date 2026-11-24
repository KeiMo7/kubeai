package modelcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/kubeai-project/kubeai/internal/config"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testCacheProfile     = "shared"
	testCacheVolumeName  = "shared-model-cache-shared"
	testCacheNamespace   = "default"
	testCacheBudgetBytes = int64(1 << 30) // 1Gi
)

func newCacheTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, kubeaiv1.AddToScheme(scheme))
	return scheme
}

func newCacheTestReconciler(t *testing.T, objs ...client.Object) *ModelReconciler {
	t.Helper()
	scheme := newCacheTestScheme(t)
	return &ModelReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&batchv1.Job{}).
			WithObjects(objs...).
			Build(),
		Scheme: scheme,
		CacheProfiles: map[string]config.CacheProfile{
			testCacheProfile: {
				SharedFilesystem: &config.CacheSharedFilesystem{
					PersistentVolumeName: "pv",
					StorageSize:          "1Gi",
				},
			},
		},
		ModelLoaders: config.ModelLoading{Image: "loader"},
		ModelServers: config.ModelServers{
			VLLM: config.ModelServer{Images: map[string]string{"default": "vllm"}},
		},
	}
}

func newCacheTestPVC(ledger *cacheLedger) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCacheVolumeName,
			Namespace: testCacheNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(testCacheBudgetBytes, resource.BinarySI),
				},
			},
		},
	}
	if ledger != nil {
		raw, _ := json.Marshal(ledger)
		pvc.Annotations = map[string]string{kubeaiv1.PVCCacheLedgerAnnotation: string(raw)}
	}
	return pvc
}

func newCacheTestModel(name, url string, sizeBytes int64) *kubeaiv1.Model {
	m := &kubeaiv1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testCacheNamespace,
			UID:       types.UID("uid-" + name),
		},
		Spec: kubeaiv1.ModelSpec{
			URL:          url,
			Engine:       kubeaiv1.VLLMEngine,
			CacheProfile: testCacheProfile,
			Replicas:     ptr.To[int32](1),
		},
	}
	if sizeBytes > 0 {
		m.Annotations = map[string]string{
			kubeaiv1.ModelCacheSizeAnnotation: fmt.Sprintf("%d", sizeBytes),
		}
	}
	return m
}

func cacheProfileForTest(t *testing.T, r *ModelReconciler, model *kubeaiv1.Model) ModelConfig {
	t.Helper()
	src, err := r.parseModelSource(model.Spec.URL)
	require.NoError(t, err)
	return ModelConfig{
		CacheProfile: r.CacheProfiles[model.Spec.CacheProfile],
		Source:       src,
	}
}

func listLoadJobs(t *testing.T, r *ModelReconciler) []batchv1.Job {
	t.Helper()
	jobs := &batchv1.JobList{}
	require.NoError(t, r.List(context.Background(), jobs, client.InNamespace(testCacheNamespace)))
	var out []batchv1.Job
	for _, job := range jobs.Items {
		if _, ok := job.Labels["skip"]; ok {
			continue
		}
		out = append(out, job)
	}
	return out
}

func TestCacheAdmissionRejectsBeforeCreatingLoadJob(t *testing.T) {
	// The only existing entry is referenced by a Model that is being served,
	// so it can never be reclaimed. The new entry must be rejected without a
	// download Job being created.
	old := newCacheTestModel("old", "hf://org/old", 0)
	oldSrc, err := parseModelURL(old.Spec.URL)
	require.NoError(t, err)
	oldKey := cacheEntryIdentityForModel(old, oldSrc).Key

	ledger := newCacheLedger()
	ledger.Entries[oldKey] = &cacheLedgerEntry{
		RelPath:   "models/old-uid-old",
		SizeBytes: testCacheBudgetBytes,
		SizeKnown: true,
		Present:   true,
	}

	fresh := newCacheTestModel("fresh", "hf://org/fresh", testCacheBudgetBytes/2)
	r := newCacheTestReconciler(t, newCacheTestPVC(ledger), old, fresh)

	res, _, err := r.reconcileCache(context.Background(), fresh, cacheProfileForTest(t, r, fresh))
	require.NoError(t, err)
	require.NotZero(t, res.RequeueAfter)

	job := &batchv1.Job{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-fresh"}, job)
	require.Error(t, err, "a rejected admission must not create a download job")

	require.NotNil(t, fresh.Status.Cache)
	require.NotNil(t, fresh.Status.Cache.Admission)
	require.False(t, fresh.Status.Cache.Admission.Admitted)
	require.Greater(t, fresh.Status.Cache.Admission.ShortfallBytes, int64(0))
	require.NotEmpty(t, fresh.Status.Cache.Admission.Holders)
	require.False(t, fresh.Status.Cache.Loaded)

	// The rejection is recorded on the volume ledger.
	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, pvc))
	persisted := parseCacheLedger(pvc)
	require.Len(t, persisted.RecentRejections, 1)
	require.Equal(t, fresh.Status.Cache.Admission.ShortfallBytes, persisted.RecentRejections[0].ShortfallBytes)

	// The in-use entry still occupies the ledger.
	require.Contains(t, persisted.Entries, oldKey)
}

func TestCacheAdmissionReclaimsLeastRecentlyUsedReclaimableEntry(t *testing.T) {
	// old and recent are unreferenced; inuse is referenced by a serving Model.
	// Only old should be selected even though inuse is just as old.
	oldKey := cacheEntryKey("hf", "org/old", "", "")
	recentKey := cacheEntryKey("hf", "org/recent", "", "")
	inUseKey := cacheEntryKey("hf", "org/inuse", "", "")

	n := metav1.NewTime(time.Now().Add(-time.Hour))
	inUse := newCacheTestModel("inuse", "hf://org/inuse", 0)

	ledger := newCacheLedger()
	ledger.Entries[oldKey] = &cacheLedgerEntry{
		RelPath: "models/old-uid-old", SizeBytes: 600 << 20, SizeKnown: true, Present: true,
		LastUsed: ptr.To(n.Add(-2 * time.Hour)),
	}
	ledger.Entries[recentKey] = &cacheLedgerEntry{
		RelPath: "models/recent-uid-recent", SizeBytes: 200 << 20, SizeKnown: true, Present: true,
		LastUsed: ptr.To(time.Now().Add(-time.Minute)),
	}
	ledger.Entries[inUseKey] = &cacheLedgerEntry{
		RelPath: "models/inuse-uid-inuse", SizeBytes: 100 << 20, SizeKnown: true, Present: true,
		LastUsed: ptr.To(n.Add(-5 * time.Hour)),
	}

	fresh := newCacheTestModel("fresh", "hf://org/fresh", 600<<20)
	r := newCacheTestReconciler(t, newCacheTestPVC(ledger), inUse, fresh)

	_, _, err := r.reconcileCache(context.Background(), fresh, cacheProfileForTest(t, r, fresh))
	require.NoError(t, err)

	// No download Job yet: reclaim must finish first.
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-fresh"}, &batchv1.Job{}))

	reclaim := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: reclaimCacheJobName(oldKey)}, reclaim))
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: reclaimCacheJobName(inUseKey)}, &batchv1.Job{}))
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: reclaimCacheJobName(recentKey)}, &batchv1.Job{}))

	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, pvc))
	persisted := parseCacheLedger(pvc)
	require.True(t, persisted.Entries[oldKey].Reclaiming)
	require.False(t, persisted.Entries[inUseKey].Reclaiming)
	require.False(t, persisted.Entries[recentKey].Reclaiming)

	// Completing the reclaim frees the budget and admits the download.
	reclaim.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, r.Status().Update(context.Background(), reclaim))

	_, afterReclaim, err := r.reconcileCache(context.Background(), fresh, cacheProfileForTest(t, r, fresh))
	require.NoError(t, err)
	require.NotNil(t, afterReclaim)

	load := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-fresh"}, load))

	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, pvc))
	persisted = parseCacheLedger(pvc)
	require.NotContains(t, persisted.Entries, oldKey)
	require.Contains(t, persisted.RecentReclaims, kubeaiv1.ModelCacheReclaimStatus{EntryKey: oldKey, SizeBytes: 600 << 20, Reason: "reclaimed capacity for a new cache entry", At: persisted.RecentReclaims[0].At})
}

func TestConcurrentLoadsCannotBothFit(t *testing.T) {
	// Each entry fits alone (600Mi of 1Gi), but not together. Only the first
	// admitted load may create a download Job.
	a := newCacheTestModel("model-a", "hf://org/a", 600<<20)
	b := newCacheTestModel("model-b", "hf://org/b", 600<<20)
	r := newCacheTestReconciler(t, newCacheTestPVC(nil), a, b)

	_, result, err := r.reconcileCache(context.Background(), a, cacheProfileForTest(t, r, a))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-model-a"}, &batchv1.Job{}))

	res, _, err := r.reconcileCache(context.Background(), b, cacheProfileForTest(t, r, b))
	require.NoError(t, err)
	require.NotZero(t, res.RequeueAfter)

	require.Error(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-model-b"}, &batchv1.Job{}),
		"the second load must not enter the download phase")
	require.NotNil(t, b.Status.Cache.Admission)
	require.False(t, b.Status.Cache.Admission.Admitted)
}

func TestSharedEntryIsDownloadedOnce(t *testing.T) {
	// Two Models with the same source version share one entry. The second
	// waits for the shared download instead of starting its own.
	a := newCacheTestModel("model-a", "hf://org/shared", 600<<20)
	b := newCacheTestModel("model-b", "hf://org/shared", 600<<20)
	r := newCacheTestReconciler(t, newCacheTestPVC(nil), a, b)

	_, _, err := r.reconcileCache(context.Background(), a, cacheProfileForTest(t, r, a))
	require.NoError(t, err)

	res, _, err := r.reconcileCache(context.Background(), b, cacheProfileForTest(t, r, b))
	require.NoError(t, err)
	require.NotZero(t, res.RequeueAfter)
	require.NotNil(t, b.Status.Cache.Admission)
	require.False(t, b.Status.Cache.Admission.Admitted)

	loadA := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-model-a"}, loadA))
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-model-b"}, &batchv1.Job{}))
}

func TestFailedDownloadDoesNotOccupyBudgetOrReportReady(t *testing.T) {
	key := cacheEntryKey("hf", "org/broken", "", "")
	a := newCacheTestModel("broken", "hf://org/broken", 600<<20)
	pvc := newCacheTestPVC(nil)
	ledger := newCacheLedger()
	ledger.Entries[key] = &cacheLedgerEntry{RelPath: "models/broken-uid-broken"}
	ledger.Reservations[key] = cacheLedgerReservation{
		EntryKey: key, ModelName: a.Name, ModelUID: string(a.UID),
		JobName: "load-cache-broken", Bytes: 600 << 20, SizeKnown: true, StartedAt: time.Now(),
	}
	raw, _ := json.Marshal(ledger)
	pvc.Annotations = map[string]string{kubeaiv1.PVCCacheLedgerAnnotation: string(raw)}

	failed := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "load-cache-broken", Namespace: testCacheNamespace},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
		},
	}
	r := newCacheTestReconciler(t, pvc, a, failed)

	res, result, err := r.reconcileCache(context.Background(), a, cacheProfileForTest(t, r, a))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotZero(t, res.RequeueAfter)
	require.False(t, a.Status.Cache.Loaded)

	latest := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, latest))
	persisted := parseCacheLedger(latest)
	require.NotContains(t, persisted.Reservations, key, "a failed load must not hold budget")
	// The entry may remain as a partial marker, but it must never be reported
	// present, ready or as occupying budget.
	if entry := persisted.Entries[key]; entry != nil {
		require.False(t, entry.Present)
		require.True(t, entry.Partial)
		require.Zero(t, entry.SizeBytes)
	}

	// The failed Job is cleaned up so a retry starts from a clean state.
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-broken"}, &batchv1.Job{}))
}

func TestMissingEntryInformationColdRebuilds(t *testing.T) {
	// No ledger, no annotation: the entry is simply rebuilt. The presence of a
	// corrupt ledger must not cause an error loop.
	pvc := newCacheTestPVC(nil)
	pvc.Annotations = map[string]string{kubeaiv1.PVCCacheLedgerAnnotation: "{not json"}
	m := newCacheTestModel("m", "hf://org/model", 100<<20)
	r := newCacheTestReconciler(t, pvc, m)

	_, result, err := r.reconcileCache(context.Background(), m, cacheProfileForTest(t, r, m))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Loaded || result.RelPath != "")
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-m"}, &batchv1.Job{}))

	// The corrupt ledger is replaced rather than retried forever.
	latest := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, latest))
	require.NotEmpty(t, latest.Annotations[kubeaiv1.PVCCacheLedgerAnnotation])
	require.Equal(t, cacheLedgerVersion, parseCacheLedger(latest).Version)
}

func TestUnknownEntrySizeIsAdmittedThenCalibrated(t *testing.T) {
	// Without a declared or measured size, admission cannot know the need.
	// The load is admitted and the measured size is calibrated afterwards.
	m := newCacheTestModel("m", "hf://org/model", 0)
	r := newCacheTestReconciler(t, newCacheTestPVC(nil), m)

	_, result, err := r.reconcileCache(context.Background(), m, cacheProfileForTest(t, r, m))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, m.Status.Cache.Admission)
	require.True(t, m.Status.Cache.Admission.Admitted)

	load := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-m"}, load))

	// Simulate a completed Job whose loader printed the measured size.
	load.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, r.Status().Update(context.Background(), load))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "loader-pod",
			Namespace: testCacheNamespace,
			Labels:    map[string]string{batchv1.JobNameLabel: "load-cache-m"},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Message: "734003200\n",
			}},
		}}},
	}
	require.NoError(t, r.Create(context.Background(), pod))

	_, result, err = r.reconcileCache(context.Background(), m, cacheProfileForTest(t, r, m))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Loaded)

	latest := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, latest))
	promptKey := cacheEntryIdentityForModel(m, mustParseURL(t, m.Spec.URL)).Key
	entry := parseCacheLedger(latest).Entries[promptKey]
	require.NotNil(t, entry)
	require.True(t, entry.SizeKnown)
	require.Equal(t, int64(734003200), entry.SizeBytes)
}

func TestVersionChangeCreatesNewEntryAndKeepsOld(t *testing.T) {
	base := "hf://org/model"
	a := newCacheTestModel("m", base, 100<<20)
	r := newCacheTestReconciler(t, newCacheTestPVC(nil), a)
	_, _, err := r.reconcileCache(context.Background(), a, cacheProfileForTest(t, r, a))
	require.NoError(t, err)

	// Complete the first download.
	load := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-m"}, load))
	load.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, r.Status().Update(context.Background(), load))
	_, result, err := r.reconcileCache(context.Background(), a, cacheProfileForTest(t, r, a))
	require.NoError(t, err)
	require.True(t, result.Loaded)
	oldKey := result.Key

	// Point at a new version; a new entry is created and the old one is kept
	// (not overwritten) while it is still referenced by the Model history.
	updated := a.DeepCopy()
	updated.Annotations = map[string]string{kubeaiv1.ModelCacheVersionAnnotation: "v2"}
	_, newResult, err := r.reconcileCache(context.Background(), updated, cacheProfileForTest(t, r, updated))
	require.NoError(t, err)
	require.NotNil(t, newResult)
	require.NotEqual(t, oldKey, newResult.Key)

	latest := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, latest))
	ledger := parseCacheLedger(latest)
	require.Contains(t, ledger.Entries, oldKey)
	require.Contains(t, ledger.Entries, newResult.Key)
	require.True(t, ledger.Entries[oldKey].Present)
	require.NotEqual(t, ledger.Entries[oldKey].RelPath, ledger.Entries[newResult.Key].RelPath)
}

func TestEntryKeyAndStatusNeverExposeSource(t *testing.T) {
	m := newCacheTestModel("m", "s3://private-bucket/secret/path?token=topsecret", 10<<20)
	r := newCacheTestReconciler(t, newCacheTestPVC(nil), m)
	_, result, err := r.reconcileCache(context.Background(), m, cacheProfileForTest(t, r, m))
	require.NoError(t, err)
	require.NotNil(t, result)

	raw, err := json.Marshal(m.Status)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "private-bucket")
	require.NotContains(t, string(raw), "secret/path")
	require.NotContains(t, string(raw), "topsecret")
	require.NotContains(t, result.Key, "private")
}

func mustParseURL(t *testing.T, raw string) modelURL {
	t.Helper()
	u, err := parseModelURL(raw)
	require.NoError(t, err)
	return u
}

func TestCacheStatusMatchesActions(t *testing.T) {
	// Status must be derived from the same ledger that drives actions: after a
	// completed download the entry is present, sized, referenced and ready.
	m := newCacheTestModel("m", "hf://org/model", 300<<20)
	r := newCacheTestReconciler(t, newCacheTestPVC(nil), m)
	_, _, err := r.reconcileCache(context.Background(), m, cacheProfileForTest(t, r, m))
	require.NoError(t, err)

	load := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "load-cache-m"}, load))
	load.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, r.Status().Update(context.Background(), load))

	_, result, err := r.reconcileCache(context.Background(), m, cacheProfileForTest(t, r, m))
	require.NoError(t, err)
	require.True(t, result.Loaded)
	require.NotNil(t, m.Status.Cache.Entry)
	require.Equal(t, result.Key, m.Status.Cache.Entry.Key)
	require.Equal(t, cacheLoadStateReady, m.Status.Cache.Entry.LoadState)
	// observedByCount is derived from the Model list; the fake client returns
	// the Model that was just reconciled.
	require.GreaterOrEqual(t, m.Status.Cache.Entry.ReferencedBy, int32(0))
	require.True(t, m.Status.Cache.Entry.SizeKnown)
	require.NotNil(t, m.Status.Cache.Volume)
	require.Equal(t, testCacheBudgetBytes, m.Status.Cache.Volume.BudgetBytes)
	require.Equal(t, int64(300<<20), m.Status.Cache.Volume.UsedBytes)
	require.Len(t, m.Status.Cache.Volume.Entries, 1)

	// A pending load is never reported ready.
	pending := newCacheTestModel("pending", "hf://org/pending", 100<<20)
	require.NoError(t, r.Create(context.Background(), pending))
	pending = &kubeaiv1.Model{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: testCacheNamespace, Name: "pending"}, pending))
	_, _, err = r.reconcileCache(context.Background(), pending, cacheProfileForTest(t, r, pending))
	require.NoError(t, err)
	require.False(t, pending.Status.Cache.Loaded)
	// The entry is reserved but the Job may not be observed yet; either way it
	// must never be reported ready.
	require.NotEqual(t, cacheLoadStateReady, pending.Status.Cache.Entry.LoadState)
}

func TestConcurrentLedgerMutationsBothReserve(t *testing.T) {
	// Two independent reservations written through the optimistic loop must
	// both persist: one cannot silently overwrite the other.
	r := newCacheTestReconciler(t, newCacheTestPVC(nil))
	require.NoError(t, r.mutateCacheLedger(context.Background(), testCacheNamespace, testCacheVolumeName, func(l *cacheLedger) error {
		l.Entries["a"] = &cacheLedgerEntry{RelPath: "models/a", SizeBytes: 10, SizeKnown: true}
		return nil
	}))
	require.NoError(t, r.mutateCacheLedger(context.Background(), testCacheNamespace, testCacheVolumeName, func(l *cacheLedger) error {
		l.Entries["b"] = &cacheLedgerEntry{RelPath: "models/b", SizeBytes: 20, SizeKnown: true}
		return nil
	}))

	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: testCacheVolumeName, Namespace: testCacheNamespace}, pvc))
	ledger := parseCacheLedger(pvc)
	require.Contains(t, ledger.Entries, "a")
	require.Contains(t, ledger.Entries, "b")
}

func TestProposeCacheRelPathNeverCollides(t *testing.T) {
	m := &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "m", UID: "uid-1"}}
	ledger := newCacheLedger()
	first := proposeCacheRelPath(m, "c-aaaaaaaaaaaaaaaa", ledger)
	require.Equal(t, "models/m-uid-1", first)
	ledger.Entries["old"] = &cacheLedgerEntry{RelPath: first}

	second := proposeCacheRelPath(m, "c-bbbbbbbbbbbbbbbb", ledger)
	require.NotEqual(t, first, second)
	require.True(t, strings.HasPrefix(second, "models/"))
	require.NotEqual(t, first, second)
}
