// Copyright 2016 Google Inc. All Rights Reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"math"
	"strconv"

	v1 "k8s.io/api/core/v1"
)

const (
	// Weight for placement constraint
	PlacementCostWeight float64 = 100
)

// Queue is a queue
type Processor interface {
	// Filter nodes for the particular pod
	Filter(pod *v1.Pod, nodes []*NodeInfo) []*NodeInfo
	// Score feasable nodes
	Score(pod *v1.Pod, nodes []*NodeInfo) []*NodeInfo
	// Choose best node
	Choose([]*NodeInfo) *NodeInfo
	// Bind the chosen node
	Bind(pod *v1.Pod, node *v1.Node)
}

type processorImpl struct{}

// Constraints, annotations and resources
func (p *processorImpl) Filter(pod *v1.Pod, nodes []*NodeInfo) []*NodeInfo {
	resource := calculateResource(pod)
	newNodes := []*NodeInfo{}
	for _, node := range nodes {
		if pod.ObjectMeta.Annotations["constraints/color"] != node.node.GetObjectMeta().GetAnnotations()["constraints/color"] {
			continue
		}

		if resource.MilliCPU <= node.Allocatable.MilliCPU && resource.Memory <= node.Allocatable.Memory {
			newNodes = append(newNodes, node)
		}
	}
	return newNodes
}

func (p *processorImpl) Score(pod *v1.Pod, nodes []*NodeInfo) []*NodeInfo {
	// Process nodes and score based on some criteria
	podPlacementCost, _ := pod.Annotations["pod/placement/cost"]
	podPlacementCostFloat, _ := strconv.ParseFloat(podPlacementCost, 32)

	for _, node := range nodes {
		node.Score = p.ScoreResources(node)

		placementResource, ok := node.node.Annotations["node/placement/resource"]
		if !ok {
			continue
		}
		placementResourceFloat, err := strconv.ParseFloat(placementResource, 32)
		if err != nil {
			continue
		}
		node.Score += int64(PlacementCostWeight * (placementResourceFloat - podPlacementCostFloat))
	}
	return nodes
}

func (p *processorImpl) Choose(nodes []*NodeInfo) *NodeInfo {
	// Choose node based on internal score
	if len(nodes) == 0 {
		return nil
	}

	bestNode := nodes[0]
	bestScore := int64(math.MaxInt64)
	for _, node := range nodes {
		if node.Score > bestScore {
			bestScore = node.Score
			bestNode = node
		}
	}
	return bestNode
}

func (p *processorImpl) Bind(pod *v1.Pod, node *v1.Node) {
	BindPod(pod, node)
}

func (p *processorImpl) ScoreResources(node *NodeInfo) int64 {
	return node.Allocatable.MilliCPU + node.Allocatable.Memory*DefaultMilliCPURequest/DefaultMemoryRequest
}

// New is a new instance of a Processor
func NewProcessor() Processor {
	return &processorImpl{}
}
