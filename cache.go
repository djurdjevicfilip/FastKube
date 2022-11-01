package main

import (
	"sync"

	v1 "k8s.io/api/core/v1"
)

type infoListItem struct {
	info interface{}
	next *infoListItem
	prev *infoListItem
}

func newInfoListItem(ni interface{}) *infoListItem {
	return &infoListItem{
		info: ni,
		next: nil,
		prev: nil,
	}
}

func newNodeInfoListItem(ni interface{}) *infoListItem {
	item := &infoListItem{
		info: &NodeInfo{
			Allocatable: &Resource{},
			Requested:   &Resource{}},
		next: nil,
		prev: nil,
	}

	item.info.(*NodeInfo).SetNode(ni.(*v1.Node))
	return item
}

type Cache interface {
	Add(key string, v interface{} /* pod or node */)
	Update(key string, v interface{} /* pod or node */)
	Delete(key string)
	Pop() *infoListItem
	TakeNodeSnapshot() []*NodeInfo
}

type cacheImpl struct {
	head   *infoListItem
	tail   *infoListItem
	podMap map[string]*infoListItem
	lock   sync.RWMutex
	cond   sync.Cond
}

func (q *cacheImpl) Add(key string, v interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	n := q.CreateInfoListItem(v)

	if q.head == nil {
		q.head = n
		q.tail = q.head
	} else {
		tmp := q.head
		q.head = n
		tmp.prev = q.head
		q.head.next = tmp
	}

	q.podMap[key] = n
	q.cond.Broadcast()
}

// CALLER MUST HOLD LOCK
func (q *cacheImpl) ManipulateHoldsLock(key string) interface{} {
	n, ok := q.podMap[key]
	if ok {
		return n.info
	}
	return nil
}

func (q *cacheImpl) CreateInfoListItem(v interface{}) *infoListItem {
	switch v := v.(type) {
	case *v1.Pod:
		return newInfoListItem(v)
	case *v1.Node:
		return newNodeInfoListItem(v)
	}
	return nil
}

// Does this work? Really!?
func (q *cacheImpl) Update(key string, v interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.podMap[key]; ok {
		q.podMap[key] = q.CreateInfoListItem(v)
	}
}

func (q *cacheImpl) Delete(key string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if el, ok := q.podMap[key]; ok {
		q.RemoveItemHoldsLock(el)
		delete(q.podMap, key)
	}
}

// Internal - Caller already holds lock
func (q *cacheImpl) RemoveItemHoldsLock(item *infoListItem) {

	prev := item.prev
	next := item.next

	if prev != nil {
		prev.next = next
	} else {
		q.head = next
	}

	if next != nil {
		next.prev = prev
	} else {
		q.tail = prev
	}
}

func (q *cacheImpl) TakeNodeSnapshot() []*NodeInfo {
	q.lock.Lock()
	defer q.lock.Unlock()
	nodeSnapshot := []*NodeInfo{}
	for node := q.head; node != nil; node = node.next {
		nodeSnapshot = append(nodeSnapshot, node.info.(*NodeInfo).Clone())
	}
	return nodeSnapshot
}

func (q *cacheImpl) Pop() *infoListItem {
	// RW lock
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.head == nil {
		// Wait for new element
		q.cond.Wait()
	}

	info := q.head
	q.RemoveItemHoldsLock(info)
	switch v := info.info.(type) {
	case *v1.Pod:
		delete(q.podMap, v.GetObjectMeta().GetName())
	case *v1.Node:
		delete(q.podMap, v.GetObjectMeta().GetName())
	}

	return info
}

// New is a new instance of a cache
func NewCache() *cacheImpl {
	q := &cacheImpl{
		head:   nil,
		tail:   nil,
		podMap: make(map[string]*infoListItem)}

	q.cond.L = &q.lock
	return q
}
