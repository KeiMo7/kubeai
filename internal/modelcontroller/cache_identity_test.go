package modelcontroller

import (
	"testing"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCacheEntryKeyStableAndShared(t *testing.T) {
	src, err := parseModelURL("hf://org/model")
	require.NoError(t, err)

	// Object recreation (new name/UID) must not change the key.
	m1 := &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "a", UID: "uid-a"}}
	m2 := &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "b", UID: "uid-b"}}
	require.Equal(t, cacheEntryIdentityForModel(m1, src).Key, cacheEntryIdentityForModel(m2, src).Key)

	// Query parameters that carry credentials do not affect the key.
	withToken, err := parseModelURL("hf://org/model?token=secret")
	require.NoError(t, err)
	require.Equal(t, cacheEntryIdentityForModel(m1, src).Key, cacheEntryIdentityForModel(m1, withToken).Key)

	// The key never exposes the source address or a credential.
	key := cacheEntryIdentityForModel(m1, src).Key
	require.NotContains(t, key, "org")
	require.NotContains(t, key, "model")
	require.NotContains(t, key, "secret")
}

func TestCacheEntryKeyVersionChange(t *testing.T) {
	v1, err := parseModelURL("hf://org/model")
	require.NoError(t, err)
	m := &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
	key1 := cacheEntryIdentityForModel(m, v1).Key

	// An explicit version annotation points the Model at new source content
	// and therefore changes the key.
	annotated := m.DeepCopy()
	annotated.Annotations = map[string]string{kubeaiv1.ModelCacheVersionAnnotation: "v3"}
	require.NotEqual(t, key1, cacheEntryIdentityForModel(annotated, v1).Key)

	// oci tags participate in the version.
	oci1, err := parseModelURL("oci://registry/org/model:1.0")
	require.NoError(t, err)
	oci2, err := parseModelURL("oci://registry/org/model:2.0")
	require.NoError(t, err)
	require.NotEqual(t, cacheEntryIdentityForModel(m, oci1).Key, cacheEntryIdentityForModel(m, oci2).Key)
}

func TestCacheEntryReclaimableOnlyWhenUnreferencedAndIdle(t *testing.T) {
	entry := &cacheEntryView{Key: "k", Present: true, SizeBytes: 100}
	require.True(t, entry.Reclaimable())

	entry.References = []string{"model-a"}
	require.False(t, entry.Reclaimable())

	entry.References = nil
	entry.ServingPods = 1
	require.False(t, entry.Reclaimable())

	entry.ServingPods = 0
	entry.Reclaiming = true
	require.False(t, entry.Reclaimable())

	entry.Reclaiming = false
	entry.Present = false
	require.False(t, entry.Reclaimable(), "an entry that is not present is never reclaimed")
}

func TestCacheEntryKeyIgnoresUnsafeURLParts(t *testing.T) {
	// The Model URL is validated by the CRD; the identity code must still not
	// panic or echo anything for a malformed URL.
	_, err := parseModelURL("iNv@lid://path/to/model")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "iNv@lid")
}
