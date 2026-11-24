package modelcontroller

import (
	"testing"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlacementInvalidatedWhenEntryReclaimed(t *testing.T) {
	// A conclusion tied to an entry that was reclaimed must be reported stale.
	model := &kubeaiv1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "m"},
		Status: kubeaiv1.ModelStatus{Cache: &kubeaiv1.ModelStatusCache{
			Loaded: true,
			Placement: &kubeaiv1.ModelPlacementStatus{
				EntryKey: "c-old",
				Valid:    true,
				Replicas: []kubeaiv1.ModelPlacementReplicaStatus{{Pod: "m-0", Node: "node-a", Reason: "ready"}},
			},
		}},
	}
	r := &ModelReconciler{}
	r.invalidatePlacement(model, &cacheReconcileResult{Key: "c-old", Entry: &cacheEntryView{Key: "c-old", Reclaiming: true}})

	placement := model.Status.Cache.Placement
	require.False(t, placement.Valid)
	require.Equal(t, "c-old", placement.EntryKey)
	require.Equal(t, placementReasonDeferred, placement.Reason)
	// Existing replicas are not touched by invalidation.
	require.Empty(t, placement.Replicas)
}

func TestPlacementInvalidationKeepsEntryKey(t *testing.T) {
	model := &kubeaiv1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "m"},
		Status:     kubeaiv1.ModelStatus{Cache: &kubeaiv1.ModelStatusCache{}},
	}
	r := &ModelReconciler{}
	r.invalidatePlacement(model, &cacheReconcileResult{Key: "c-new"})
	require.NotNil(t, model.Status.Cache.Placement)
	require.False(t, model.Status.Cache.Placement.Valid)
	require.Equal(t, "c-new", model.Status.Cache.Placement.EntryKey)
}
