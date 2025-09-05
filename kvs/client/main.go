package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func runClient(id int, clients []*Client, batchSize int, hosts []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	len_hosts := len(hosts)
	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)

	// Batch get requests for each host. When we see a put, we process the
	// batch of gets for that host and then issue the put.
	batchGetsForHost := make([]kvs.BatchGetRequest, len_hosts)
	sendBatchGets := func(hostId int) {
		request := batchGetsForHost[hostId]
		response := kvs.BatchGetResponse{Responses: make([]kvs.GetResponse, len(request.Requests))}
		if err := clients[hostId].rpcClient.Call("KVService.BatchGet", &request, &response); err != nil {
			log.Fatal(err)
		}
		atomic.AddUint64(&opsCompleted, uint64(len(response.Responses)))
		batchGetsForHost[hostId].Requests = batchGetsForHost[hostId].Requests[:0]
	}

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			hostId := int(op.Key % uint64(len_hosts))
			if op.IsRead {
				request := kvs.GetRequest{Key: key}
				batchGetsForHost[hostId].Requests = append(batchGetsForHost[hostId].Requests, request)
			} else {
				sendBatchGets(hostId)
				clients[hostId].Put(key, value)
				atomic.AddUint64(&opsCompleted, 1)
			}
		}

		var batchWg sync.WaitGroup
		for hostId := 0; hostId < len_hosts; hostId++ {
			batchWg.Add(1)
			go func(hId int) {
				defer batchWg.Done()
				sendBatchGets(hId)
			}(hostId)
		}
		batchWg.Wait()
	}

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- atomic.LoadUint64(&opsCompleted)
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
	numClients := flag.Int("clients", 256, "Concurrent clients")       // Best number is around 1024 - 1100
	batchSize := flag.Int("batchSize", 1024, "Get request batch size") // for max throughput currently is 10000
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	connectionSets := make([][]*Client, *numClients)
	for i := 0; i < *numClients; i++ {
		connectionSets[i] = make([]*Client, len(hosts))
		for j, addr := range hosts {
			connectionSets[i][j] = Dial(addr)
		}
	}

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64, *numClients)
	var wg sync.WaitGroup

	for clientId := 0; clientId < *numClients; clientId++ {
		wg.Add(1)
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, connectionSets[clientId], *batchSize, hosts, &done, workload, resultsCh, &wg)
		}(clientId)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)
	wg.Wait()
	close(resultsCh)

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
