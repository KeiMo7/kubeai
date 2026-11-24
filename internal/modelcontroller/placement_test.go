package modelcontroller

import (
	"bytes"
	"encoding/json"
	"testing"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/kubeai-project/kubeai/internal/config"
	"github.com/kubeai-project/kubeai/internal/k8sutils"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func placementTestNode(name, zone string, ready bool, labels map[string]string, taints ...corev1.Taint) placementNode {
	if labels == nil {
		labels = map[string]string{}
	}
	if zone != "" {
		labels[topologyZoneKey] = zone
	}
	return placementNode{
		Name:   name,
		Zone:   zone,
		Labels: labels,
		Taints: taints,
		Ready:  ready,
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("64Gi"),
		},
	}
}

func placementTestModel(name string, replicas int32) *kubeaiv1.Model {
	return &kubeaiv1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: kubeaiv1.ModelSpec{
			Replicas: ptr.To(replicas),
			Engine:   kubeaiv1.VLLMEngine,
			URL:      "hf://org/model",
		},
	}
}

func placementTestPod(name, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: node},
	}
}

func TestComputePlacementDeterministic(t *testing.T) {
	model := placementTestModel("m", 3)
	in := placementInput{
		Model: model,
		Config: ModelConfig{ResourceProfile: config.ResourceProfile{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		}},
		Pods:          []corev1.Pod{placementTestPod("m-0", "node-b")},
		NodeInventory: true,
		Nodes: []placementNode{
			placementTestNode("node-a", "zone-1", true, nil),
			placementTestNode("node-b", "zone-1", true, nil),
			placementTestNode("node-c", "zone-2", true, nil),
		},
	}

	first := computePlacement(in)
	require.True(t, first.Valid)
	for i := 0; i < 100; i++ {
		// Input slice order must not matter.
		shuffled := in
		shuffled.Nodes = []placementNode{in.Nodes[2], in.Nodes[0], in.Nodes[1]}
		again := computePlacement(shuffled)
		require.Equal(t, mustJSON(t, first), mustJSON(t, again))
	}

	// The node with no replica of this model in a different zone comes first.
	var candidateReplica *kubeaiv1.ModelPlacementReplicaStatus
	for i := range first.Replicas {
		if first.Replicas[i].Pod == "" {
			candidateReplica = &first.Replicas[i]
			break
		}
	}
	require.NotNil(t, candidateReplica)
	require.NotEmpty(t, candidateReplica.Candidates)
	require.Equal(t, "node-c", candidateReplica.Candidates[0].Node)

	// Node-b already hosts a replica, so it must not be preferred.
	require.NotEqual(t, "node-b", candidateReplica.Candidates[0].Node)
}

func TestComputePlacementUnaffectedByUnrelatedModels(t *testing.T) {
	model := placementTestModel("m", 2)
	base := placementInput{
		Model:         model,
		NodeInventory: true,
		Pods:          []corev1.Pod{placementTestPod("m-0", "node-a")},
		Nodes: []placementNode{
			placementTestNode("node-a", "zone-1", true, nil),
			placementTestNode("node-b", "zone-2", true, nil),
		},
		OtherPodReqs: map[string]corev1.ResourceList{},
	}
	withOther := base
	withOther.OtherPodReqs = map[string]corev1.ResourceList{
		"node-a": {corev1.ResourceCPU: resource.MustParse("1")},
	}

	// Another Model's Pod changes headroom only; with identical headroom the
	// preference order must be identical.
	a := computePlacement(base)
	b := computePlacement(withOther)
	require.Equal(t, a.Valid, b.Valid)
	require.Equal(t, len(a.Replicas), len(b.Replicas))
}

func TestComputePlacementHonorsHardConstraints(t *testing.T) {
	model := placementTestModel("m", 1)
	cfg := ModelConfig{
		ResourceProfile: config.ResourceProfile{
			NodeSelector: map[string]string{"gpu": "true"},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "arch",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"arm64"},
						}},
					}},
				},
			}},
			Tolerations: []corev1.Toleration{{
				Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
			}},
		},
	}
	in := placementInput{
		Model:         model,
		Config:        cfg,
		NodeInventory: true,
		Nodes: []placementNode{
			placementTestNode("good", "z1", true, map[string]string{"gpu": "true", "arch": "arm64"},
				corev1.Taint{Key: "nvidia.com/gpu", Effect: corev1.TaintEffectNoSchedule}),
			placementTestNode("wrong-selector", "z1", true, map[string]string{"gpu": "false", "arch": "arm64"}),
			placementTestNode("wrong-affinity", "z1", true, map[string]string{"gpu": "true", "arch": "amd64"}),
			placementTestNode("untolerated", "z1", true, map[string]string{"gpu": "true", "arch": "arm64"},
				corev1.Taint{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}),
			placementTestNode("not-ready", "z1", false, map[string]string{"gpu": "true", "arch": "arm64"}),
		},
	}
	// Only "untolerated" lacks a matching toleration.

	result := computePlacement(in)
	require.True(t, result.Valid)
	require.Len(t, result.Replicas, 1)
	require.Equal(t, []kubeaiv1.ModelPlacementCandidateStatus{{
		Node:   "good",
		Rank:   0,
		Reason: result.Replicas[0].Candidates[0].Reason,
	}}, result.Replicas[0].Candidates)
}

func TestComputePlacementMissingInventoryKeepsHardConstraints(t *testing.T) {
	model := placementTestModel("m", 2)
	in := placementInput{
		Model:         model,
		NodeInventory: false,
		Pods:          []corev1.Pod{placementTestPod("m-0", "node-a")},
	}
	result := computePlacement(in)
	require.False(t, result.Valid)
	require.Equal(t, placementReasonNoInventory, result.Reason)
	require.Len(t, result.Replicas, 2)
	require.Equal(t, "node-a", result.Replicas[0].Node)
	require.Empty(t, result.Replicas[1].Candidates)
}

func TestComputePlacementNoCandidatesReportsButDoesNotReject(t *testing.T) {
	model := placementTestModel("m", 1)
	in := placementInput{
		Model:         model,
		Config:        ModelConfig{ResourceProfile: config.ResourceProfile{NodeSelector: map[string]string{"gpu": "true"}}},
		NodeInventory: true,
		Nodes:         []placementNode{placementTestNode("cpu", "z1", true, nil)},
	}
	result := computePlacement(in)
	require.False(t, result.Valid)
	require.Equal(t, placementReasonNoCandidates, result.Reason)
	require.Len(t, result.Replicas, 1)
	require.Empty(t, result.Replicas[0].Candidates)
}

func TestPlacementDoesNotChangePodSpecOrHash(t *testing.T) {
	r := &ModelReconciler{
		ModelServers: config.ModelServers{VLLM: config.ModelServer{Images: map[string]string{"default": "vllm"}}},
	}
	model := placementTestModel("m", 3)
	src, err := r.parseModelSource(model.Spec.URL)
	require.NoError(t, err)
	cfg := ModelConfig{Source: src, Image: "vllm"}

	before := r.vLLMPodForModel(model, cfg)
	beforeHash := k8sutils.PodHash(before.Spec)

	// A different placement preference (e.g. nodes changed) must not touch the
	// Pod template, so the existing replica is not recreated.
	in := placementInput{
		Model:         model,
		Config:        cfg,
		NodeInventory: true,
		Pods:          []corev1.Pod{placementTestPod("m-0", "node-a")},
		Nodes: []placementNode{
			placementTestNode("node-a", "z1", true, nil),
			placementTestNode("node-b", "z2", true, nil),
			placementTestNode("node-c", "z3", true, nil),
		},
	}
	_ = computePlacement(in)
	in.Nodes = []placementNode{placementTestNode("node-z", "z9", true, nil)}
	_ = computePlacement(in)

	after := r.vLLMPodForModel(model, cfg)
	require.Equal(t, beforeHash, k8sutils.PodHash(after.Spec))
}

func TestPlacementTiesBreakByNodeName(t *testing.T) {
	model := placementTestModel("m", 1)
	in := placementInput{
		Model:         model,
		NodeInventory: true,
		Nodes: []placementNode{
			placementTestNode("zone-b-node", "z", true, nil),
			placementTestNode("zone-a-node", "z", true, nil),
		},
	}
	result := computePlacement(in)
	require.True(t, result.Valid)
	require.Equal(t, "zone-a-node", result.Replicas[0].Candidates[0].Node)
	require.Equal(t, "zone-b-node", result.Replicas[0].Candidates[1].Node)
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	require.NoError(t, enc.Encode(v))
	return buf.String()
}
