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
func ByteViewSink(dst *ByteView) Sink {
	if dst == nil {
		panic("nil dst")
	}
	return &byteViewSink{dst: dst}
}

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
}

func (s *byteViewSink) setView(v ByteView) error {
	*s.dst = v
	return nil
}

func (s *byteViewSink) view() (ByteView, error) {
	return *s.dst, nil
}

func (s *byteViewSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	*s.dst = ByteView{b: b}
	return nil
}

func (s *byteViewSink) SetBytes(b []byte) error {
	*s.dst = ByteView{b: cloneBytes(b)}
	return nil
}

func (s *byteViewSink) SetString(v string) error {
	*s.dst = ByteView{s: v}
	return nil
}

// ProtoSink returns a sink that unmarshals binary proto values into m.
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

func (s *protoSink) view() (ByteView, error) {
	return s.v, nil
}

func (s *protoSink) SetBytes(b []byte) error {
	err := proto.Unmarshal(b, s.dst)
	if err != nil {
		return err
	}
	s.v.b = cloneBytes(b)
	s.v.s = ""
	return nil
}

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
func AllocatingByteSliceSink(dst *[]byte) Sink {
	return &allocBytesSink{dst: dst}
}

type allocBytesSink struct {
	dst *[]byte
	v   ByteView
}

func (s *allocBytesSink) view() (ByteView, error) {
	return s.v, nil
}

func (s *allocBytesSink) setView(v ByteView) error {
	if v.b != nil {
		*s.dst = cloneBytes(v.b)
	} else {
		*s.dst = []byte(v.s)
	}
	s.v = v
	return nil
}

func (s *allocBytesSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return s.setBytesOwned(b)
}

func (s *allocBytesSink) SetBytes(b []byte) error {
	return s.setBytesOwned(cloneBytes(b))
}

func (s *allocBytesSink) setBytesOwned(b []byte) error {
	if s.dst == nil {
		return errors.New("nil AllocatingByteSliceSink *[]byte dst")
	}
	*s.dst = cloneBytes(b) // another copy, protecting the read-only s.v.b view
	s.v.b = b
	s.v.s = ""
	return nil
}

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
