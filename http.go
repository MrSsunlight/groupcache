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

package groupcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/groupcache/consistenthash"
	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
// 实现 PeerPicker 的 http 池
type HTTPPool struct {
	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, the server uses the request's context
	// Context可选地指定服务器在接收请求时要使用的上下文。如果为空，则服务器使用请求的上下文
	Context func(*http.Request) context.Context

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	// Transport可选地指定http。当客户端发出请求时使用的RoundTripper。如果为空，客户端使用http.DefaultTransport
	Transport func(context.Context) http.RoundTripper

	// this peer's base URL, e.g. "https://example.net:8000"
	// 这个 peer 的基础URL
	self string

	// opts specifies the options.
	// 指定的选项
	opts HTTPPoolOptions

	// 保护peer和httpGetters
	mu          sync.Mutex // guards peers and httpGetters
	// 一致性哈希
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// HTTPPoolOptions are the configurations of a HTTPPool.
// HTTPPool 的配置
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	// 指定将服务于groupache请求的HTTP路径，如果为空，则默认为 defaultBasePath：/_groupcache/
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	// 指定一致散列上的密钥副本的数量，如果为空，则默认为 defaultReplicas：50
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	// 指定一致哈希的哈希函数，如果为空，则默认为crc32。ChecksumIEEE
	HashFn consistenthash.Hash
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
// 初始化一个 HTTP 对等池，并将自己注册为 PeerPicker。 为方便起见，它还使用 http.DefaultServeMux 将自己注册为一个 http.Handler。
// self 参数应该是指向当前服务器的有效基本 URL，例如“http://example.net:8000”
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

// 标志 NewHTTPPool 是否被调用
var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
// NewHTTPPoolOpts使用给定选项初始化HTTP对等体池。与NewHTTPPool不同，此函数不会将创建的池注册为HTTP处理程序。
// 返回的 *HTTPPool 实现了 http.Handler 并且必须使用 http.Handle 注册
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	// 判断是否传入 否则使用默认
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	// 初始化一致性哈希环
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	// 注册peer
	RegisterPeerPicker(func() PeerPicker { return p })
	// 返回 httpPool
	return p
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
// 更新池的peer列表。 每个peer应该是一个有效的基本 URL，例如“http://example.net:8000”。
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 创建缓冲池
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	// 将服务器添加到缓存池
	p.peers.Add(peers...)
	//
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{transport: p.Transport, baseURL: peer + p.opts.BasePath}
	}
}

// 根据 key 选择 peer
func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

// 获取对应url 的 response
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	// 解析请求
	// url 校验
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// 服务器、虚拟节点？？？
	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
	// 获取 group 值
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	// 关联上下文
	var ctx context.Context
	if p.Context != nil {
		ctx = p.Context(r)
	} else {
		ctx = r.Context()
	}

	// 请求计数
	group.Stats.ServerRequests.Add(1)
	var value []byte
	err := group.Get(ctx, key, AllocatingByteSliceSink(&value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the value to the response body as a proto message.
	// 将该值作为原始消息写入响应正文
	body, err := proto.Marshal(&pb.GetResponse{Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// 拼head 以及 body
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

//
type httpGetter struct {
	// 链路
	transport func(context.Context) http.RoundTripper
	// 基础 URL
	baseURL   string
}

// 内存池
var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// 从url链路获取数据，并写入pb 数据结构中
func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	// 拼装完整链路
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	// 建立请求
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	// 初始化请求参数
	req = req.WithContext(ctx)
	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(ctx)
	}
	// 设置重传次数
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	// 查看响应状态码
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	// 获取响应数据
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	// 数据写入pb结构
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}
