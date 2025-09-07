package kvs

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

type PutRequest struct {
	Key   string
	Value string
}
type PutResponse struct{}

type Operation struct {
	OpType string // "GET" or "PUT"
	Key    string
	Value  string
}

type BatchOpRequest struct {
	Operations []Operation
}

type BatchOpResponse struct {
	Results []string
}
