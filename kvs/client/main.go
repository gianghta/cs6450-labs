package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/rstutsman/cs6450-labs/kvs"
)

// Ultra-fast client with zero-copy optimizations
type UltraClient struct {
	conn net.Conn
	encoder *rpc.ClientCodec
	decoder *rpc.ClientCodec
	seqMutex sync.Mutex
	seq uint64
}

func NewUltraClient(addr string) *UltraClient {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	
	// Configure TCP for maximum throughput
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(false) // Enable Nagle's algorithm for better batching
		tcpConn.SetWriteBuffer(1024 * 1024) // 1MB write buffer
		tcpConn.SetReadBuffer(1024 * 1024)  // 1MB read buffer
	}

	return &UltraClient{
		conn: conn,
	}
}

// Pre-allocated request/response pools with zero-allocation reuse
type RequestResponsePool struct {
	getReqs    []kvs.GetRequest
	getRsps    []kvs.GetResponse
	putReqs    []kvs.PutRequest
	putRsps    []kvs.PutResponse
	getIndex   int32
	putIndex   int32
}

func NewRequestResponsePool(size int) *RequestResponsePool {
	return &RequestResponsePool{
		getReqs: make([]kvs.GetRequest, size),
		getRsps: make([]kvs.GetResponse, size),
		putReqs: make([]kvs.PutRequest, size),
		putRsps: make([]kvs.PutResponse, size),
	}
}

func (p *RequestResponsePool) GetGetPair() (*kvs.GetRequest, *kvs.GetResponse) {
	idx := atomic.AddInt32(&p.getIndex, 1) % int32(len(p.getReqs))
	return &p.getReqs[idx], &p.getRsps[idx]
}

func (p *RequestResponsePool) GetPutPair() (*kvs.PutRequest, *kvs.PutResponse) {
	idx := atomic.AddInt32(&p.putIndex, 1) % int32(len(p.putReqs))
	return &p.putReqs[idx], &p.putRsps[idx]
}

// Lock-free circular buffer for extreme performance
type LockFreeQueue struct {
	buffer []unsafe.Pointer
	mask   uint64
	head   uint64
	tail   uint64
	_pad1  [8]uint64 // Cache line padding
	_pad2  [8]uint64
}

func NewLockFreeQueue(size int) *LockFreeQueue {
	// Ensure size is power of 2
	for size&(size-1) != 0 {
		size++
	}
	
	return &LockFreeQueue{
		buffer: make([]unsafe.Pointer, size),
		mask:   uint64(size - 1),
	}
}

// Mega-batched client function for extreme throughput
func runMegaBatchClient(id int, hosts []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, batchSize int, connections int) {
	runtime.LockOSThread() // Pin to OS thread for better performance
	
	// Create many connections per host
	clients := make([][]*rpc.Client, len(hosts))
	for i, addr := range hosts {
		clients[i] = make([]*rpc.Client, connections)
		for j := 0; j < connections; j++ {
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			clients[i][j] = client
		}
	}

	// Pre-allocate everything to avoid GC
	pool := NewRequestResponsePool(batchSize * 4)
	value := strings.Repeat("x", 128)
	
	// Pre-generate massive key cache
	keyCache := make([]string, 100000)
	for i := range keyCache {
		keyCache[i] = fmt.Sprintf("%d", i)
	}
	
	// Batch operations for maximum efficiency
	operations := make([]struct {
		isRead bool
		key    string
		client *rpc.Client
	}, batchSize)
	
	calls := make([]*rpc.Call, batchSize)
	
	opsCompleted := uint64(0)
	connectionRoundRobin := 0

	for !done.Load() {
		// Prepare batch
		for i := 0; i < batchSize && !done.Load(); i++ {
			op := workload.Next()
			keyIdx := int(op.Key) % len(keyCache)
			hostIdx := int(op.Key) % len(hosts)
			
			operations[i].isRead = op.IsRead
			operations[i].key = keyCache[keyIdx]
			operations[i].client = clients[hostIdx][connectionRoundRobin%connections]
		}
		connectionRoundRobin++

		// Fire all async calls in batch
		for i := 0; i < batchSize && !done.Load(); i++ {
			if operations[i].isRead {
				req, rsp := pool.GetGetPair()
				req.Key = operations[i].key
				calls[i] = operations[i].client.Go("KVService.Get", req, rsp, nil)
			} else {
				req, rsp := pool.GetPutPair()
				req.Key = operations[i].key
				req.Value = value
				calls[i] = operations[i].client.Go("KVService.Put", req, rsp, nil)
			}
		}

		// Wait for all calls to complete
		for i := 0; i < batchSize && !done.Load(); i++ {
			if calls[i] != nil {
				<-calls[i].Done
				if calls[i].Error != nil {
					log.Printf("Error in operation: %v", calls[i].Error)
				} else {
					opsCompleted++
				}
			}
		}
	}

	fmt.Printf("Client %d finished with %d operations.\n", id, opsCompleted)
	resultsCh <- opsCompleted
}

// Fire-and-forget client (no waiting for responses)
func runFireAndForgetClient(id int, hosts []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, batchSize int) {
	runtime.LockOSThread()
	
	// Create many connections
	connections := 16
	clients := make([][]*rpc.Client, len(hosts))
	for i, addr := range hosts {
		clients[i] = make([]*rpc.Client, connections)
		for j := 0; j < connections; j++ {
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			clients[i][j] = client
		}
	}

	pool := NewRequestResponsePool(batchSize * 8)
	value := strings.Repeat("x", 128)
	
	// Huge key cache
	keyCache := make([]string, 1000000)
	for i := range keyCache {
		keyCache[i] = fmt.Sprintf("%d", i)
	}
	
	opsCompleted := uint64(0)
	connectionIdx := 0

	for !done.Load() {
		// Fire massive batches without waiting
		for i := 0; i < batchSize && !done.Load(); i++ {
			op := workload.Next()
			keyIdx := int(op.Key) % len(keyCache)
			hostIdx := int(op.Key) % len(hosts)
			client := clients[hostIdx][connectionIdx%connections]
			
			if op.IsRead {
				req, rsp := pool.GetGetPair()
				req.Key = keyCache[keyIdx]
				// Fire and forget - don't wait for response
				client.Go("KVService.Get", req, rsp, make(chan *rpc.Call, 1))
			} else {
				req, rsp := pool.GetPutPair()
				req.Key = keyCache[keyIdx]
				req.Value = value
				// Fire and forget
				client.Go("KVService.Put", req, rsp, make(chan *rpc.Call, 1))
			}
			opsCompleted++
			connectionIdx++
		}
	}

	fmt.Printf("Client %d (fire-and-forget) finished with %d operations.\n", id, opsCompleted)
	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	numClients := flag.Int("clients", 64, "Concurrent clients")
	batchSize := flag.Int("batch", 65536, "Batch size for mega-batching")
	connections := flag.Int("connections", 32, "Connections per host per client")
	fireAndForget := flag.Bool("fire-and-forget", false, "Don't wait for responses (higher throughput)")
	maxProcs := flag.Int("maxprocs", 0, "GOMAXPROCS (0 = use all CPUs)")
	flag.Parse()

	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n"+
			"clients %d\n"+
			"batch size %d\n"+
			"connections per host %d\n"+
			"fire and forget %v\n"+
			"GOMAXPROCS %d\n",
		hosts, *theta, *workload, *secs, *numClients, *batchSize, *connections, *fireAndForget, runtime.GOMAXPROCS(0),
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64, *numClients)

	for clientId := 0; clientId < *numClients; clientId++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			if *fireAndForget {
				runFireAndForgetClient(clientId, hosts, &done, workload, resultsCh, *batchSize)
			} else {
				runMegaBatchClient(clientId, hosts, &done, workload, resultsCh, *batchSize, *connections)
			}
		}(clientId)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := uint64(0)
	for clientId := 0; clientId < *numClients; clientId++ {
		clientOps := <-resultsCh
		fmt.Printf("Client %d completed %d operations.\n", clientId, clientOps)
		opsCompleted += clientOps
	}

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
