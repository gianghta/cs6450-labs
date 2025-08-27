package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

type Request struct {
	IsRead bool
	Key    string
	Value  string // only for writes
}

type BatchRequest struct {
	Requests []Request
}

type Response struct {
	Value string // only for reads
}

type BatchResponse struct {
	Responses []Response
}
