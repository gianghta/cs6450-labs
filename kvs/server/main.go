package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts uint64
	gets uint64
}

type KVService struct {
	mp        sync.Map
	cache     *lru.Cache[string, string]
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.cache, _ = lru.New[string, string](128)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)
	// Check cache first
	if value, found := kv.cache.Get(request.Key); found {
		response.Value = value
		return nil
	}

	value, ok := kv.mp.Load(request.Key)
	if ok {
		response.Value = value.(string)
		kv.cache.Add(request.Key, response.Value)
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	atomic.AddUint64(&kv.stats.puts, 1)

	kv.mp.Store(request.Key, request.Value)
	kv.cache.Remove(request.Key)

	return nil
}

func (kv *KVService) printStats() {
	currentGets := atomic.LoadUint64(&kv.stats.gets)
	currentPuts := atomic.LoadUint64(&kv.stats.puts)

	now := time.Now()
	deltaS := now.Sub(kv.lastPrint).Seconds()

	diffGets := currentGets - kv.prevStats.gets
	diffPuts := currentPuts - kv.prevStats.puts

	kv.prevStats.gets = currentGets
	kv.prevStats.puts = currentPuts
	kv.lastPrint = now

	getsPerSec := float64(diffGets) / deltaS
	putsPerSec := float64(diffPuts) / deltaS
	opsPerSec := getsPerSec + putsPerSec

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		getsPerSec,
		putsPerSec,
		opsPerSec,
	)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
