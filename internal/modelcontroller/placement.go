package modelcontroller

import (
	"context"
	"sort"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/kubeai-project/kubeai/internal/k8sutils"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Placement reason strings are stable so status diffs only occur when the
// conclusion actually changes.
const (
	placementReasonDeferred     = "cache entry is not ready; placement is deferred"
	placementReasonNoInventory  = "node inventory unavailable; keeping hard constraints only"
	placementReasonNoCandidates = "no node satisfies the hard constraints"
	placementReasonExistingNode = "replica already scheduled; it is not moved"
	placementReasonUnscheduled  = "replica is waiting to be scheduled"
	placementReasonPreference   = "preferred by replica spread, fault domain and headroom"
)

const (
	topologyZoneKey   = "topology.kubernetes.io/zone"
	topologyRegionKey = "topology.kubernetes.io/region"
	legacyZoneKey     = "failure-domain.beta.kubernetes.io/zone"
	legacyRegionKey   = "failure-domain.beta.kubernetes.io/region"
)

// placementNode is the node state relevant to placement. It is a plain
// snapshot so placement is a pure function of cluster state.
type placementNode struct {
	Name          string
	Zone          string
	Region        string
	Unschedulable bool
	Ready         bool
	Labels        map[string]string
	Taints        []corev1.Taint
	Allocatable   corev1.ResourceList
}

// placementInput is a pure snapshot of everything placement depends on.
type placementInput struct {
	Model         *kubeaiv1.Model
	Config        ModelConfig
	Pods          []corev1.Pod
	Nodes         []placementNode
	NodeInventory bool
	OtherPodReqs  map[string]corev1.ResourceList
}

// placementResult is a deterministic placement conclusion.
type placementResult struct {
	Replicas []kubeaiv1.ModelPlacementReplicaStatus
	Valid    bool
	Reason   string
}

// computePlacement produces the candidate node preference order for every
// replica. It only reads cluster state: the same input always produces the
// same output, and it never mutates Pod specs, so a preference change can
// never rebuild a ready replica.
func computePlacement(in placementInput) placementResult {
	result := placementResult{Valid: true, Reason: placementReasonPreference}

	desired := int32(0)
	if in.Model.Spec.Replicas != nil {
		desired = *in.Model.Spec.Replicas
	}

	var existing []corev1.Pod
	for i := range in.Pods {
		if in.Pods[i].DeletionTimestamp != nil {
			continue
		}
		existing = append(existing, in.Pods[i])
	}
	sort.Slice(existing, func(i, j int) bool { return existing[i].Name < existing[j].Name })

	for i := range existing {
		pod := &existing[i]
		reason := placementReasonExistingNode
		if pod.Spec.NodeName == "" {
			reason = placementReasonUnscheduled
		}
		result.Replicas = append(result.Replicas, kubeaiv1.ModelPlacementReplicaStatus{
			Pod:    pod.Name,
			Node:   pod.Spec.NodeName,
			Reason: reason,
		})
	}

	missing := int(desired) - len(existing)
	if missing <= 0 {
		return result
	}

	if !in.NodeInventory || len(in.Nodes) == 0 {
		result.Valid = false
		result.Reason = placementReasonNoInventory
		for i := 0; i < missing; i++ {
			result.Replicas = append(result.Replicas, kubeaiv1.ModelPlacementReplicaStatus{
				Reason: placementReasonNoInventory,
			})
		}
		return result
	}

	// Existing replicas already consume capacity on their nodes. Account for
	// them alongside the other Models' Pods so headroom is comparable.
	if in.OtherPodReqs == nil {
		in.OtherPodReqs = map[string]corev1.ResourceList{}
	}
	modelReqs := map[string]corev1.ResourceList{}
	requests := in.Config.Requests
	for i := range existing {
		node := existing[i].Spec.NodeName
		if node == "" {
			continue
		}
		cur := modelReqs[node]
		if cur == nil {
			cur = corev1.ResourceList{}
		}
		for name, q := range requests {
			existingQ := cur[name]
			existingQ.Add(q)
			cur[name] = existingQ
		}
		modelReqs[node] = cur
	}
	for node, reqs := range modelReqs {
		other := in.OtherPodReqs[node]
		if other == nil {
			other = corev1.ResourceList{}
		}
		for name, q := range reqs {
			sum := other[name]
			sum.Add(q)
			other[name] = sum
		}
		in.OtherPodReqs[node] = other
	}

	candidates := rankPlacementCandidates(in, existing)
	if len(candidates) == 0 {
		// Candidate discovery failed; keep the hard constraints and report it
		// instead of refusing to create the replica.
		result.Valid = false
		result.Reason = placementReasonNoCandidates
		for i := 0; i < missing; i++ {
			result.Replicas = append(result.Replicas, kubeaiv1.ModelPlacementReplicaStatus{
				Reason: placementReasonNoCandidates,
			})
		}
		return result
	}

	ordered := make([]kubeaiv1.ModelPlacementCandidateStatus, 0, len(candidates))
	for i := range candidates {
		ordered = append(ordered, kubeaiv1.ModelPlacementCandidateStatus{
			Node:   candidates[i].node.Name,
			Rank:   int32(i),
			Reason: candidates[i].reason,
		})
	}
	for i := 0; i < missing; i++ {
		result.Replicas = append(result.Replicas, kubeaiv1.ModelPlacementReplicaStatus{
			Reason:     placementReasonPreference,
			Candidates: append([]kubeaiv1.ModelPlacementCandidateStatus(nil), ordered...),
		})
	}
	return result
}

type placementCandidate struct {
	node       placementNode
	reason     string
	sameModel  int
	sameZone   int
	sameRegion int
	headroom   int64
}

// rankPlacementCandidates filters nodes by the hard constraints and sorts the
// remainder by preference: fewest replicas of this Model, then fault domain
// spread, then the most free resources, then the node name.
func rankPlacementCandidates(in placementInput, existing []corev1.Pod) []placementCandidate {
	nodeIndex := make(map[string]*placementNode, len(in.Nodes))
	for i := range in.Nodes {
		nodeIndex[in.Nodes[i].Name] = &in.Nodes[i]
	}

	sameModelPerNode := map[string]int{}
	usedZones := map[string]int{}
	usedRegions := map[string]int{}
	for i := range existing {
		node := existing[i].Spec.NodeName
		if node == "" {
			continue
		}
		sameModelPerNode[node]++
		if n, ok := nodeIndex[node]; ok {
			if n.Zone != "" {
				usedZones[n.Zone]++
			}
			if n.Region != "" {
				usedRegions[n.Region]++
			}
		}
	}

	var candidates []placementCandidate
	for i := range in.Nodes {
		node := in.Nodes[i]
		if !nodeSatisfiesHardConstraints(node, in.Config) {
			continue
		}
		c := placementCandidate{
			node:       node,
			sameModel:  sameModelPerNode[node.Name],
			sameZone:   countOrZero(usedZones, node.Zone),
			sameRegion: countOrZero(usedRegions, node.Region),
			headroom:   nodeHeadroom(node, in.OtherPodReqs[node.Name], in.Config.Requests),
		}
		c.reason = candidateReason(c)
		candidates = append(candidates, c)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.sameModel != b.sameModel {
			return a.sameModel < b.sameModel
		}
		if a.sameZone != b.sameZone {
			return a.sameZone < b.sameZone
		}
		if a.sameRegion != b.sameRegion {
			return a.sameRegion < b.sameRegion
		}
		if a.headroom != b.headroom {
			return a.headroom > b.headroom
		}
		return a.node.Name < b.node.Name
	})
	return candidates
}

func countOrZero(counts map[string]int, key string) int {
	if key == "" {
		return 0
	}
	return counts[key]
}

func candidateReason(c placementCandidate) string {
	switch {
	case c.sameModel == 0 && c.sameZone == 0:
		return "spreads to a node with no replica of this model and no replica in its zone"
	case c.sameModel == 0:
		return "spreads to a node with no replica of this model"
	case c.sameZone == 0:
		return "spreads to a fault domain without a replica of this model"
	default:
		return "least loaded remaining node for this model"
	}
}

// nodeSatisfiesHardConstraints applies the existing node selector, required
// node affinity and tolerations. These are never relaxed by preferences.
func nodeSatisfiesHardConstraints(node placementNode, cfg ModelConfig) bool {
	if node.Unschedulable || !node.Ready {
		return false
	}
	for key, want := range cfg.NodeSelector {
		if node.Labels[key] != want {
			return false
		}
	}
	if affinity := cfg.Affinity; affinity != nil && affinity.NodeAffinity != nil {
		if req := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; req != nil {
			if !nodeSelectorMatches(req, node.Labels) {
				return false
			}
		}
	}
	for _, taint := range node.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !tolerates(cfg.Tolerations, taint) {
			return false
		}
	}
	return true
}

func nodeSelectorMatches(selector *corev1.NodeSelector, labels map[string]string) bool {
	if len(selector.NodeSelectorTerms) == 0 {
		return true
	}
	for _, term := range selector.NodeSelectorTerms {
		if nodeSelectorTermMatches(term, labels) {
			return true
		}
	}
	return false
}

func nodeSelectorTermMatches(term corev1.NodeSelectorTerm, labels map[string]string) bool {
	for _, req := range term.MatchExpressions {
		if !nodeSelectorRequirementMatches(req, labels) {
			return false
		}
	}
	// MatchFields are left to the scheduler: keeping the node is the safe
	// choice because the hard constraint is still enforced there.
	return true
}

func nodeSelectorRequirementMatches(req corev1.NodeSelectorRequirement, labels map[string]string) bool {
	value, ok := labels[req.Key]
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		if !ok {
			return false
		}
		for _, v := range req.Values {
			if v == value {
				return true
			}
		}
		return false
	case corev1.NodeSelectorOpNotIn:
		if !ok {
			return true
		}
		for _, v := range req.Values {
			if v == value {
				return false
			}
		}
		return true
	case corev1.NodeSelectorOpExists:
		return ok
	case corev1.NodeSelectorOpDoesNotExist:
		return !ok
	default:
		// Numeric comparisons (Gt/Lt) are not represented in the snapshot.
		// Keeping the node is safe because the scheduler still enforces them.
		return true
	}
}

func tolerates(tolerations []corev1.Toleration, taint corev1.Taint) bool {
	for i := range tolerations {
		t := tolerations[i]
		if t.Effect != "" && t.Effect != taint.Effect {
			continue
		}
		if t.Key != "" && t.Key != taint.Key {
			continue
		}
		switch t.Operator {
		case corev1.TolerationOpExists:
			return true
		case corev1.TolerationOpEqual, "":
			if t.Value == taint.Value {
				return true
			}
		}
	}
	return false
}

// nodeHeadroom estimates the free resources of a node for this Model after
// accounting for already observed Pods and requested capacity.
func nodeHeadroom(node placementNode, otherRequests, modelRequests corev1.ResourceList) int64 {
	var headroom int64
	keys := map[corev1.ResourceName]struct{}{}
	for key := range node.Allocatable {
		keys[key] = struct{}{}
	}
	for key := range modelRequests {
		keys[key] = struct{}{}
	}
	for key := range keys {
		alloc := quantityOrZero(node.Allocatable, key)
		used := quantityOrZero(otherRequests, key)
		free := alloc.DeepCopy()
		free.Sub(used)
		if free.Sign() < 0 {
			free.Set(0)
		}
		// Headroom is reported in the units of the scarcest matching resource
		// so the comparison stays meaningful across CPU and memory.
		if headroom == 0 || free.Value() < headroom {
			headroom = free.Value()
		}
	}
	return headroom
}

// reconcilePlacement computes and stores the placement conclusion for a Model.
// It never changes the Pod template, so ready replicas are not recreated when
// preferences change.
func (r *ModelReconciler) reconcilePlacement(ctx context.Context, model *kubeaiv1.Model, cfg ModelConfig, entry *cacheEntryView, volume string, pods []corev1.Pod) {
	if model.Spec.CacheProfile == "" {
		return
	}
	if model.Status.Cache == nil {
		model.Status.Cache = &kubeaiv1.ModelStatusCache{}
	}

	status := &kubeaiv1.ModelPlacementStatus{Volume: volume}
	if entry == nil || !entry.Present || entry.Reclaiming {
		status.Valid = false
		status.Reason = placementReasonDeferred
		if entry != nil {
			status.EntryKey = entry.Key
		}
		model.Status.Cache.Placement = status
		return
	}
	status.EntryKey = entry.Key

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		model.Status.Cache.Placement = &kubeaiv1.ModelPlacementStatus{
			EntryKey: entry.Key,
			Volume:   volume,
			Valid:    false,
			Reason:   placementReasonNoInventory,
			Replicas: existingPlacementReplicas(pods, placementReasonNoInventory),
		}
		return
	}

	otherReqs := map[string]corev1.ResourceList{}
	allPods := &corev1.PodList{}
	if err := r.List(ctx, allPods, client.InNamespace(model.Namespace)); err == nil {
		for i := range allPods.Items {
			pod := &allPods.Items[i]
			if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
				continue
			}
			if k8sutils.GetLabel(pod, kubeaiv1.PodModelLabel) == model.Name {
				continue
			}
			cur := otherReqs[pod.Spec.NodeName]
			if cur == nil {
				cur = corev1.ResourceList{}
			}
			for name, q := range resourceListRequests(pod) {
				existing := cur[name]
				existing.Add(q)
				cur[name] = existing
			}
			otherReqs[pod.Spec.NodeName] = cur
		}
	}

	in := placementInput{
		Model:         model,
		Config:        cfg,
		Pods:          pods,
		Nodes:         placementNodes(nodeList.Items),
		NodeInventory: true,
		OtherPodReqs:  otherReqs,
	}
	result := computePlacement(in)
	status.Valid = result.Valid
	status.Reason = result.Reason
	status.Replicas = result.Replicas
	model.Status.Cache.Placement = status
}

func existingPlacementReplicas(pods []corev1.Pod, reason string) []kubeaiv1.ModelPlacementReplicaStatus {
	var out []kubeaiv1.ModelPlacementReplicaStatus
	for i := range pods {
		if pods[i].DeletionTimestamp != nil {
			continue
		}
		out = append(out, kubeaiv1.ModelPlacementReplicaStatus{
			Pod:    pods[i].Name,
			Node:   pods[i].Spec.NodeName,
			Reason: reason,
		})
	}
	return out
}

func placementNodes(nodes []corev1.Node) []placementNode {
	out := make([]placementNode, 0, len(nodes))
	for i := range nodes {
		node := &nodes[i]
		out = append(out, placementNode{
			Name:          node.Name,
			Zone:          firstLabel(node.Labels, topologyZoneKey, legacyZoneKey),
			Region:        firstLabel(node.Labels, topologyRegionKey, legacyRegionKey),
			Unschedulable: node.Spec.Unschedulable,
			Ready:         nodeReady(node),
			Labels:        node.Labels,
			Taints:        node.Spec.Taints,
			Allocatable:   node.Status.Allocatable,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func nodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func firstLabel(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if v, ok := labels[key]; ok && v != "" {
			return v
		}
	}
	return ""
}
