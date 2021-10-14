/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package consistenthash provides an implementation of a ring hash.
// 一致性哈希实现
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 函数 返回0-2^32-1
type Hash func(data []byte) uint32

type Map struct {
	// 函数
	hash     Hash
	// 虚拟节点个数
	replicas int
	// 哈希环上的点列表
	keys     []int // Sorted
	// 哈希环上点到服务器名的映射
	hashMap  map[int]string
}

/*
	参数：虚拟节点个数以及哈希函数
	返回：Map 指针
*/
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	// 默认哈希函数
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no items available.
// 判断 Map 是否为空
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// Add adds some keys to the hash.
// 将缓存服务器加到Map中
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		// 遍历虚拟节点
		for i := 0; i < m.replicas; i++ {
			// key + 编号 算哈希值
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			// 将点的哈希值 添加到 keys 内
			m.keys = append(m.keys, hash)
			// 将虚拟节点关联到服务器上
			m.hashMap[hash] = key
		}
	}
	// 升序排列虚拟节点
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
// 获取key 要存到哪个服务器上，返回服务器名称
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	// 计算 key 对应的哈希值
	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica.
	// 利用二分查找 找到 m.keys[i] >= hash 的最小虚拟节点
	// 找不到返回默认 len(m.keys)，否则返回环上的 i 未知，i前面的节点都不满足 >= hash， 后面的节点都满足 >= hash；
	// 也就是哈希环上顺时针 离这个要存储key 最新的一个缓存服务器
	idx := sort.Search(len(m.keys),
		func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	// 如果idx = len(m.keys) 也就是说上面没有找到，将idx 设置成 0
	if idx == len(m.keys) {
		idx = 0
	}

	// 返回用来存 key 的服务器
	return m.hashMap[m.keys[idx]]
}
