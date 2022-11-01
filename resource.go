package main

import v1 "k8s.io/api/core/v1"

// For each of these resources, a pod that doesn't request the resource explicitly
// will be treated as having requested the amount indicated below, for the purpose
// of computing priority only.
const (
	// DefaultMilliCPURequest defines default milli cpu request number.
	DefaultMilliCPURequest int64 = 100 // 0.1 core
	// DefaultMemoryRequest defines default memory request size.
	DefaultMemoryRequest int64 = 200 * 1024 * 1024 // 200 MB
)

// Resource is a collection of compute resource.
type Resource struct {
	MilliCPU int64
	Memory   int64
}

// NewResource creates a Resource from ResourceList
func NewResource(rl v1.ResourceList) *Resource {
	r := &Resource{}
	r.Add(rl)
	return r
}

// Add adds ResourceList into Resource.
func (r *Resource) Add(rl v1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.MilliCPU += rQuant.MilliValue()
		case v1.ResourceMemory:
			r.Memory += rQuant.Value()
		default:
		}
	}
}

// Clone returns a copy of this resource.
func (r *Resource) Clone() *Resource {
	res := &Resource{
		MilliCPU: r.MilliCPU,
		Memory:   r.Memory,
	}
	return res
}

// resourceRequest = sum(podSpec.Containers)
func calculateResource(pod *v1.Pod) (res Resource) {
	resPtr := &res
	for _, c := range pod.Spec.Containers {
		resPtr.Add(c.Resources.Requests)
	}

	return
}

// GetNonzeroRequests returns the default cpu and memory resource request if none is found or
// what is provided on the request.
func GetNonzeroRequests(requests *v1.ResourceList) (int64, int64) {
	return GetRequestForResource(v1.ResourceCPU, requests, true),
		GetRequestForResource(v1.ResourceMemory, requests, true)
}

// GetRequestForResource returns the requested values unless nonZero is true and there is no defined request
// for CPU and memory.
// If nonZero is true and the resource has no defined request for CPU or memory, it returns a default value.
func GetRequestForResource(resource v1.ResourceName, requests *v1.ResourceList, nonZero bool) int64 {
	if requests == nil {
		return 0
	}
	switch resource {
	case v1.ResourceCPU:
		// Override if un-set, but not if explicitly set to zero
		if _, found := (*requests)[v1.ResourceCPU]; !found && nonZero {
			return DefaultMilliCPURequest
		}
		return requests.Cpu().MilliValue()
	case v1.ResourceMemory:
		// Override if un-set, but not if explicitly set to zero
		if _, found := (*requests)[v1.ResourceMemory]; !found && nonZero {
			return DefaultMemoryRequest
		}
		return requests.Memory().Value()
	case v1.ResourceEphemeralStorage:
		quantity, found := (*requests)[v1.ResourceEphemeralStorage]
		if !found {
			return 0
		}
		return quantity.Value()
	default:
		quantity, found := (*requests)[resource]
		if !found {
			return 0
		}
		return quantity.Value()
	}
}

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	// Overall node information.
	node *v1.Node

	// Total requested resources of all pods on this node. This does not include assumed
	// pods, which scheduler has sent for binding, but may not be scheduled yet.
	Requested *Resource

	/// Capacity available for pods
	Allocatable *Resource

	// Score determined by the Processor object
	Score int64
}

func (n *NodeInfo) Clone() *NodeInfo {
	return &NodeInfo{
		node:        n.node,
		Allocatable: n.Allocatable.Clone(),
		Requested:   n.Requested.Clone(),
	}
}

// SetNode sets the overall node information.
func (n *NodeInfo) SetNode(node *v1.Node) {
	n.node = node
	n.Allocatable = NewResource(node.Status.Allocatable)
}

func (n *NodeInfo) AddRequestedResources(pod *v1.Pod) {
	res := calculateResource(pod)
	n.Requested.MilliCPU += res.MilliCPU
	n.Requested.Memory += res.Memory
}

func (n *NodeInfo) RemoveRequestedResources(pod *v1.Pod) {
	res := calculateResource(pod)
	n.Requested.MilliCPU -= res.MilliCPU
	n.Requested.Memory -= res.Memory
}
