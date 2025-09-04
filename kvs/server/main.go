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

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts atomic.Uint64
	gets atomic.Uint64
}

type PrevStats struct {
	puts uint64
	gets uint64
}

type KVService struct {
	mp        sync.Map
	stats     Stats
	prevStats PrevStats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.stats.gets.Add(1)

	value, ok := kv.mp.Load(request.Key)
	if ok {
		response.Value = value.(string)
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.stats.puts.Add(1)
	kv.mp.Store(request.Key, request.Value)

	return nil
}

func (kv *KVService) BatchGet(request *kvs.BatchGetRequest, response *kvs.BatchGetResponse) error {
	numGets := len(request.Requests)

	response.Responses = make([]kvs.GetResponse, numGets)
	for i, req := range request.Requests {
		if value, ok := kv.mp.Load(req.Key); ok {
			response.Responses[i].Value = value.(string)
		}
	}

	kv.stats.gets.Add(uint64(numGets))
	return nil
}

func (kv *KVService) printStats() {
	currentGets := kv.stats.gets.Load()
	currentPuts := kv.stats.puts.Load()

	now := time.Now()
	deltaS := now.Sub(kv.lastPrint).Seconds()

	diffGets := currentGets - kv.prevStats.gets
	diffPuts := currentPuts - kv.prevStats.puts

	kv.prevStats.gets = currentGets
	kv.prevStats.puts = currentPuts
	kv.lastPrint = now

	fmt.Printf("get/s %.2f\nput/s %.2f\nops/s %.2f\n\n",
		float64(diffGets)/deltaS,
		float64(diffPuts)/deltaS,
		float64(diffGets+diffPuts)/deltaS)
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
