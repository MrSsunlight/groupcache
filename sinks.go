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

package groupcache

import (
	"errors"

	"github.com/golang/protobuf/proto"
)

// A Sink receives data from a Get call.

// Implementation of Getter must call exactly one of the Set methods
// on success.
// Sink 从 Get 调用接收数据。
// Getter 实现必须调用下面的某个Set方法。
type Sink interface {
	// SetString sets the value to s.
	// 将值设置成 s
	SetString(s string) error

	// SetBytes sets the value to the contents of v.
	// The caller retains ownership of v.
	// 调用者保留 v 的所有权
	SetBytes(v []byte) error

	// SetProto sets the value to the encoded version of m.
	// The caller retains ownership of m.
	// 调用者保留 m 的所有权
	SetProto(m proto.Message) error

	// view returns a frozen view of the bytes for caching.
	// 返回缓存字节的冻结视图， 注意byteview 类型在返回值里
	view() (ByteView, error)
}

// 克隆一个 byte 切片
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func setSinkView(s Sink, v ByteView) error {
	// A viewSetter is a Sink that can also receive its value from
	// a ByteView. This is a fast path to minimize copies when the
	// item was already cached locally in memory (where it's
	// cached as a ByteView)
	// 一个 viewsetter 是一个实现了sink 接口的类型，所以可以接收到一个 byteview 值
	// 当一个条目缓存在内存中时，这种方式的复制量最少
	type viewSetter interface {
		setView(v ByteView) error
	}
	if vs, ok := s.(viewSetter); ok {
		return vs.setView(v)
	}
	if v.b != nil {
		return s.SetBytes(v.b)
	}
	return s.SetString(v.s)
}

// StringSink returns a Sink that populates the provided string pointer.
// 根据获取到的字符串指针 构造一个stringsink 结构体实例
func StringSink(sp *string) Sink {
	return &stringSink{sp: sp}
}

// 两个成员：一个字符串指针，一个byteview 类型
// stringsink 实现了 sink接口
type stringSink struct {
	sp *string
	v  ByteView
	// TODO(bradfitz): track whether any Sets were called.
}

// 获取stringsink 的 byteview
func (s *stringSink) view() (ByteView, error) {
	// TODO(bradfitz): return an error if no Set was called
	return s.v, nil
}

// 设置stringsink 子符串属性
func (s *stringSink) SetString(v string) error {
	s.v.b = nil
	s.v.s = v
	*s.sp = v
	return nil
}

// 将 v 放进去
func (s *stringSink) SetBytes(v []byte) error {
	return s.SetString(string(v))
}

// 从 pb 中获取数据，然后放入stringsink 的 sp 和 v.b
func (s *stringSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	s.v.b = b
	*s.sp = string(b)
	return nil
}

// ByteViewSink returns a Sink that populates a ByteView.
// 根据参数dst构造 byteViewSink 结构体实例
func ByteViewSink(dst *ByteView) Sink {
	if dst == nil {
		panic("nil dst")
	}
	return &byteViewSink{dst: dst}
}

// 属性dst为一个ByteView指针
type byteViewSink struct {
	dst *ByteView

	// if this code ever ends up tracking that at least one set*
	// method was called, don't make it an error to call set
	// methods multiple times. Lorry's payload.go does that, and
	// it makes sense. The comment at the top of this file about
	// "exactly one of the Set methods" is overly strict. We
	// really care about at least once (in a handler), but if
	// multiple handlers fail (or multiple functions in a program
	// using a Sink), it's okay to re-use the same one.
	// 如果此代码最终跟踪到至少调用了一个 set* 方法，请不要将多次调用 set 方法视为错误。
	// Lorry 的 payload.go 做到了这一点，这是有道理的。 此文件顶部关于“正好是 Set 方法之一”的注释过于严格。
	// 我们真正关心至少一次（在处理程序中），但是如果多个处理程序失败（或使用 Sink 的程序中的多个函数），可以重用相同的一个
}

func (s *byteViewSink) setView(v ByteView) error {
	*s.dst = v
	return nil
}

func (s *byteViewSink) view() (ByteView, error) {
	return *s.dst, nil
}

// 设置 byteViewSink 中 ByteView 的 b
func (s *byteViewSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	*s.dst = ByteView{b: b}
	return nil
}

// 复制b，初始化 byteViewSink 的 dst
func (s *byteViewSink) SetBytes(b []byte) error {
	*s.dst = ByteView{b: cloneBytes(b)}
	return nil
}

// 通过使用string类型的v初始化一个 ByteView 后初始化 byteViewSink 的 dst
func (s *byteViewSink) SetString(v string) error {
	*s.dst = ByteView{s: v}
	return nil
}

// ProtoSink returns a sink that unmarshals binary proto values into m.
// 使用proto.Message类型的m初始化protoSink的dst
func ProtoSink(m proto.Message) Sink {
	return &protoSink{
		dst: m,
	}
}

type protoSink struct {
	dst proto.Message // authoritative value
	typ string

	v ByteView // encoded
}

// 返回protoSink的ByteView类型的v
func (s *protoSink) view() (ByteView, error) {
	return s.v, nil
}

// 将s.dst反序列化后丢给b，并且复制一份丢给 protoSink 中 ByteView 的 b
func (s *protoSink) SetBytes(b []byte) error {
	err := proto.Unmarshal(b, s.dst)
	if err != nil {
		return err
	}
	s.v.b = cloneBytes(b)
	s.v.s = ""
	return nil
}

// 将b解码后写入s.dst
func (s *protoSink) SetString(v string) error {
	b := []byte(v)
	err := proto.Unmarshal(b, s.dst)
	if err != nil {
		return err
	}
	s.v.b = b
	s.v.s = ""
	return nil
}

// 将m写入 protoSink 的 dst
func (s *protoSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	// TODO(bradfitz): optimize for same-task case more and write
	// right through? would need to document ownership rules at
	// the same time. but then we could just assign *dst = *m
	// here. This works for now:
	err = proto.Unmarshal(b, s.dst)
	if err != nil {
		return err
	}
	s.v.b = b
	s.v.s = ""
	return nil
}

// AllocatingByteSliceSink returns a Sink that allocates
// a byte slice to hold the received value and assigns
// it to *dst. The memory is not retained by groupcache.
// 分配一个字节切片来保存接收到的数据
func AllocatingByteSliceSink(dst *[]byte) Sink {
	return &allocBytesSink{dst: dst}
}

// 有一个字节切片指针类型的属性dst
type allocBytesSink struct {
	dst *[]byte
	v   ByteView
}

func (s *allocBytesSink) view() (ByteView, error) {
	return s.v, nil
}

// 设置allocBytesSink的v，同时复制v中的b或者s丢给dst
func (s *allocBytesSink) setView(v ByteView) error {
	if v.b != nil {
		*s.dst = cloneBytes(v.b)
	} else {
		*s.dst = []byte(v.s)
	}
	s.v = v
	return nil
}

// 这个得从下面的setBytesOwned开始往上看
func (s *allocBytesSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return s.setBytesOwned(b)
}

// 复制一份b，然后调用setBytesOwned
func (s *allocBytesSink) SetBytes(b []byte) error {
	return s.setBytesOwned(cloneBytes(b))
}

// 使用b设置allocBytesSink的dst和ByteView
func (s *allocBytesSink) setBytesOwned(b []byte) error {
	if s.dst == nil {
		return errors.New("nil AllocatingByteSliceSink *[]byte dst")
	}
	*s.dst = cloneBytes(b) // another copy, protecting the read-only s.v.b view
	s.v.b = b
	s.v.s = ""
	return nil
}

// 字符串转成[]byte后进行和上面类似的操作
func (s *allocBytesSink) SetString(v string) error {
	if s.dst == nil {
		return errors.New("nil AllocatingByteSliceSink *[]byte dst")
	}
	*s.dst = []byte(v)
	s.v.b = nil
	s.v.s = v
	return nil
}

// TruncatingByteSliceSink returns a Sink that writes up to len(*dst)
// bytes to *dst. If more bytes are available, they're silently
// truncated. If fewer bytes are available than len(*dst), *dst
// is shrunk to fit the number of bytes available.
// 截面字节切片Sink，写入len(*dst)个字节到dst，如果长度超了会被截断，少了则dst会收缩长度来适配
func TruncatingByteSliceSink(dst *[]byte) Sink {
	return &truncBytesSink{dst: dst}
}

type truncBytesSink struct {
	dst *[]byte
	v   ByteView
}

func (s *truncBytesSink) view() (ByteView, error) {
	return s.v, nil
}

// 从下面的setBytesOwned开始看
func (s *truncBytesSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return s.setBytesOwned(b)
}

func (s *truncBytesSink) SetBytes(b []byte) error {
	return s.setBytesOwned(cloneBytes(b))
}

func (s *truncBytesSink) setBytesOwned(b []byte) error {
	if s.dst == nil {
		return errors.New("nil TruncatingByteSliceSink *[]byte dst")
	}
	n := copy(*s.dst, b)
	if n < len(*s.dst) {
		*s.dst = (*s.dst)[:n]
	}
	s.v.b = b
	s.v.s = ""
	return nil
}

func (s *truncBytesSink) SetString(v string) error {
	if s.dst == nil {
		return errors.New("nil TruncatingByteSliceSink *[]byte dst")
	}
	n := copy(*s.dst, v)
	if n < len(*s.dst) {
		*s.dst = (*s.dst)[:n]
	}
	s.v.b = nil
	s.v.s = v
	return nil
}
