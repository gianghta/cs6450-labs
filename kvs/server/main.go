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
	mp        sync.Map
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	value, ok := kv.mp.Load(request.Key)
	if ok {
		response.Value = value.(string)
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	atomic.AddUint64(&kv.stats.puts, 1)
	kv.mp.Store(request.Key, request.Value)

	return nil
}

func (kv *KVService) BatchOp(request *kvs.BatchOpRequest, response *kvs.BatchOpResponse) error {
	numOps := len(request.Operations)
	if numOps == 0 {
		return nil
	}

	response.Results = make([]string, numOps)
	var gets, puts uint64

	for i, op := range request.Operations {
		if op.OpType == "GET" {
			gets++
			if value, ok := kv.mp.Load(op.Key); ok {
				response.Results[i] = value.(string)
			}
		} else if op.OpType == "PUT" {
			puts++
			kv.mp.Store(op.Key, op.Value)
		}
	}

	if gets > 0 {
		atomic.AddUint64(&kv.stats.gets, gets)
	}
	if puts > 0 {
		atomic.AddUint64(&kv.stats.puts, puts)
	}
	return nil
}

func (kv *KVService) printStats() {
	currentGets := atomic.LoadUint64(&kv.stats.gets)
	currentPuts := atomic.LoadUint64(&kv.stats.puts)

	stats := Stats{puts: currentPuts, gets: currentGets}
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
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
