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
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	api_v1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

const maxRetries = 5

var serverStartTime time.Time

// Scheduler object
type Scheduler struct {
	clientset       kubernetes.Interface
	queue           workqueue.RateLimitingInterface
	podInformer     cache.SharedIndexInformer
	nodeInformer    cache.SharedIndexInformer
	schedulingQueue *cacheImpl
	nodeCache       *cacheImpl
}

// New returns a Scheduler
func NewScheduler() (*Scheduler, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	config, err := kubeconfig.ClientConfig()
	if err != nil {
		logrus.Fatalf("Can not get kubernetes config: %v", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		logrus.Fatalf("Can not create kubernetes client: %v", err)
	}

	schedulingQueue := NewCache()
	nodeCache := NewCache()

	// Shared cache with other controllers
	podInformer := cache.NewSharedIndexInformer(
		// Listing all pods
		// ListWatch will list all nodes if it determines that something went wrong with the connection
		// Check all pods, and after they're reported deduce the action
		&cache.ListWatch{
			ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
				return client.CoreV1().Pods(meta_v1.NamespaceAll).List(context.Background(), options)
			},
			WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
				return client.CoreV1().Pods(meta_v1.NamespaceAll).Watch(context.Background(), options)
			},
		},
		&api_v1.Pod{},
		0,
		cache.Indexers{},
	)

	nodeInformer := cache.NewSharedIndexInformer(
		// Listing all nodes
		// ListWatch will list all nodes if it determines that something went wrong with the connection
		&cache.ListWatch{
			ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
				return client.CoreV1().Nodes().List(context.Background(), options)
			},
			WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
				return client.CoreV1().Nodes().Watch(context.Background(), options)
			},
		},
		&api_v1.Node{},
		0,
		cache.Indexers{},
	)

	stopNodeNotReadyCh := make(chan struct{})
	defer close(stopNodeNotReadyCh)

	sched := newScheduler(client, podInformer, nodeInformer, schedulingQueue, nodeCache)

	// Add Event Handlers
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sched.addPodToSchedulingQueue,
		UpdateFunc: sched.updatePodInSchedulingQueue,
		DeleteFunc: sched.deletePodFromSchedulingQueue,
	})

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sched.addNodeToCache,
		UpdateFunc: sched.updateNodeInCache,
		DeleteFunc: sched.deleteNodeFromCache,
	})

	sched.Run(stopNodeNotReadyCh)
	return sched, nil
}

// Run will start the controller.
// StopCh channel is used to send interrupt signal to stop it.
func (c *Scheduler) Run(stopCh <-chan struct{}) {
	// don't let panics crash the process
	defer utilruntime.HandleCrash()

	// Run informers
	go c.podInformer.Run(stopCh)
	go c.nodeInformer.Run(stopCh)

	// wait for the caches to synchronize before starting the worker
	// will call re-list on the API if needed
	if !cache.WaitForCacheSync(stopCh, c.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	// runWorker will loop until "something bad" happens.  The .Until will
	// then rekick the worker after one second
	wait.Until(c.runWorker, time.Second, stopCh)
}

func (c *Scheduler) runWorker() {
	// processNextWorkItem will automatically wait until there's work available
	for c.processNextItem() {
		// continue looping
	}
}

// processNextWorkItem deals with one key off the queue.
func (c *Scheduler) processNextItem() bool {

	// pull the next work item from queue.  It should be a key we use to lookup
	// something in a cache
	pod := c.schedulingQueue.Pop().info.(*v1.Pod)

	// Take a copy of the current node state
	nodeSnapshot := c.nodeCache.TakeNodeSnapshot()
	processor := NewProcessor()

	feasableNodes := processor.Filter(pod, nodeSnapshot)
	feasableNodes = processor.Score(pod, feasableNodes)

	chosenNodeInfo := processor.Choose(feasableNodes)
	fmt.Printf("\n Scheduled pod  %s to node %s because\n", pod.Name, chosenNodeInfo.node.Name)

	processor.Bind(pod, chosenNodeInfo.node)

	return true
}

func (c *Scheduler) AddPodToNode(pod *v1.Pod) {
	// Take lock on nodeCache
	c.nodeCache.lock.Lock()
	defer c.nodeCache.lock.Unlock()
	n := c.nodeCache.ManipulateHoldsLock(pod.Spec.NodeName)

	// In our case pod exists only if node exists
	n.(*NodeInfo).AddRequestedResources(pod)
}

// In this case just remove pod resources
func (c *Scheduler) RemovePodFromNode(pod *v1.Pod) {
	// Take lock on nodeCache
	c.nodeCache.lock.Lock()
	defer c.nodeCache.lock.Unlock()
	n := c.nodeCache.ManipulateHoldsLock(pod.Spec.NodeName)

	// In our case pod exists only if node exists
	n.(*NodeInfo).RemoveRequestedResources(pod)
}

// HasSynced is required for the cache.Controller interface.
func (c *Scheduler) HasSynced() bool {
	return c.podInformer.HasSynced() && c.nodeInformer.HasSynced()
}

func (c *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	pod := obj.(*v1.Pod)

	if assignedPod(pod) {
		// Pod already running/assigned to node
		c.AddPodToNode(pod) // Adds pod resources
	} else {
		// Unscheduled pod
		// Adds to queue and its internal map
		c.schedulingQueue.Add(pod.GetObjectMeta().GetName(), pod)
	}
}

func (c *Scheduler) updatePodInSchedulingQueue(oldObj, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	pod := newObj.(*v1.Pod)
	if assignedPod(pod) {
		// Pod already running/assigned to node
		c.RemovePodFromNode(oldPod) // Removes pod resources
		c.AddPodToNode(pod)         // Adds pod resources
	} else {
		// Unscheduled pod
		// Adds to queue and its internal map
		c.schedulingQueue.Delete(pod.GetObjectMeta().GetName())
		c.schedulingQueue.Add(pod.GetObjectMeta().GetName(), pod)
	}
}

func (c *Scheduler) deletePodFromSchedulingQueue(obj interface{}) {
	pod := obj.(*v1.Pod)
	c.schedulingQueue.Delete(pod.GetObjectMeta().GetName())
	if assignedPod(pod) {
		// Pod already running/assigned to node
		c.RemovePodFromNode(pod) // Removes pod resources
	} else {
		// Unscheduled pod
		// Adds to queue and its internal map
		c.schedulingQueue.Delete(pod.GetObjectMeta().GetName())
	}
}

func (c *Scheduler) addNodeToCache(obj interface{}) {
	node := obj.(*v1.Node)
	c.nodeCache.Add(node.GetObjectMeta().GetName(), node)
}

func (c *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
	node := newObj.(*v1.Node)
	c.nodeCache.Update(node.GetObjectMeta().GetName(), node)
}

func (c *Scheduler) deleteNodeFromCache(obj interface{}) {
	node := obj.(*v1.Node)
	c.nodeCache.Delete(node.GetObjectMeta().GetName())
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func newScheduler(client kubernetes.Interface, podInformer cache.SharedIndexInformer, nodeInformer cache.SharedIndexInformer, schedulingQueue *cacheImpl, nodeCache *cacheImpl) *Scheduler {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	return &Scheduler{
		clientset:       client,
		podInformer:     podInformer,
		nodeInformer:    nodeInformer,
		queue:           queue,
		schedulingQueue: schedulingQueue,
		nodeCache:       nodeCache,
	}
}
