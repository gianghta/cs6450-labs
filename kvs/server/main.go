package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

const (
	numMapShards = 256
)

type Shard struct {
	sync.RWMutex
	items map[string]string
}

type Stats struct {
	sync.Mutex
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	return r
}

type KVService struct {
	shards    [numMapShards]Shard
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	for i := 0; i < numMapShards; i++ {
		kvs.shards[i].items = make(map[string]string)
	}
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) getShard(key string) *Shard {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return &kv.shards[hasher.Sum64()%numMapShards]
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.stats.Lock()
	kv.stats.gets++
	kv.stats.Unlock()

	shard := kv.getShard(request.Key)
	shard.RLock()
	value, ok := shard.items[request.Key]
	shard.RUnlock()

	if ok {
		response.Value = value
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.stats.Lock()
	kv.stats.puts++
	kv.stats.Unlock()

	shard := kv.getShard(request.Key)
	shard.Lock()
	shard.items[request.Key] = request.Value
	shard.Unlock()

	return nil
}

type batchItem struct {
	request       kvs.GetRequest
	originalIndex int
}

func (kv *KVService) BatchGet(request *kvs.BatchGetRequest, response *kvs.BatchGetResponse) error {
	groupedByShard := make(map[uint64][]batchItem, numMapShards)
	for i, req := range request.Requests {
		hasher := fnv.New64a()
		hasher.Write([]byte(req.Key))
		shardIndex := hasher.Sum64() % numMapShards
		groupedByShard[shardIndex] = append(groupedByShard[shardIndex], batchItem{req, i})
	}
	response.Responses = make([]kvs.GetResponse, len(request.Requests))

	for shardIndex, items := range groupedByShard {
		shard := &kv.shards[shardIndex]
		shard.RLock()
		for _, item := range items {
			if value, ok := shard.items[item.request.Key]; ok {
				response.Responses[item.originalIndex].Value = value
			}
		}
		shard.RUnlock()
	}

	if len(request.Requests) > 0 {
		kv.stats.Lock()
		kv.stats.gets += uint64(len(request.Requests))
		kv.stats.Unlock()
	}
	return nil
}

func (kv *KVService) printStats() {
	kv.stats.Lock()
	currentStats := Stats{
		puts: kv.stats.puts,
		gets: kv.stats.gets,
	}
	kv.stats.Unlock()

	now := time.Now()
	deltaS := now.Sub(kv.lastPrint).Seconds()
	diff := currentStats.Sub(&kv.prevStats)

	kv.prevStats.Lock()
	kv.prevStats.puts = currentStats.puts
	kv.prevStats.gets = currentStats.gets
	kv.prevStats.Unlock()

	kv.lastPrint = now

	fmt.Printf("get/s %.2f\nput/s %.2f\nops/s %.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
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
