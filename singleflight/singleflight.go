/*
Copyright 2012 Google Inc.

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

// Package singleflight provides a duplicate function call suppression mechanism.
// 提供了一个重复函数调用抑制机制
package singleflight

import "sync"

// call is an in-flight or completed Do call
// 在执行的或者已经完成的Do过程
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
// 表示一类工作，组成一个命名空间的概念，一个group的调用会有“重复抑制”
type Group struct {
	mu sync.Mutex       // protects m
	// 懒惰地初始化；这个map的value是*call，call是上面那个struct
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// Do接收一个函数，执行并返回结果，
// 这个过程中确保同一个key在同一时间只有一个执行过程；
// 重复的调用会等待最原始的调用过程完成，然后接收到相同的结果
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 如果这个call存在同名过程，等待初始调用完成，然后返回val和err
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		// 当所有goroutine执行完毕，call中就存储了执行结果val和err，然后这里返回
		return c.val, c.err
	}
	// 拿到call结构体类型的指针
	c := new(call)
	// 一个goroutine开始，Add(1)，这里最多只会执行到一次，也就是不会并发调用下面的fn()
	c.wg.Add(1)
	// 类似设置一个函数调用的名字“key”对应调用过程c
	g.m[key] = c
	g.mu.Unlock()

	// 函数调用过程
	c.val, c.err = fn()
	// 这里的Done对应上面if里面的Wait
	c.wg.Done()

	g.mu.Lock()
	// 执行完成，删除这个key
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
