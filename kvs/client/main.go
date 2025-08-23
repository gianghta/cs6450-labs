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

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	client := Dial(addr)
	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)

	for !done.Load() {
		op := workload.Next()
		key := fmt.Sprintf("%d", op.Key)
		if op.IsRead {
			client.Get(key)
		} else {
			client.Put(key, value)
		}
		opsCompleted++
	}

	fmt.Printf("Client %d finished operations.\n", id)

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
	numClients := flag.Int("clients", 32, "Concurrent clients")
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

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64, *numClients)
	var wg sync.WaitGroup

	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			host := hosts[clientId%len(hosts)]
			runClient(clientId, host, &done, workload, resultsCh, &wg)
		}(i)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	wg.Wait()
	close(resultsCh)

	var opsCompleted uint64 = 0
	for ops := range resultsCh {
		opsCompleted += ops
	}

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
