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

type BatchGetRequest struct {
	Requests []GetRequest
}

type BatchGetResponse struct {
	Responses []GetResponse
}
