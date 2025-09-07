package kvs

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

type PutRequest struct {
    Key   string
    Value string
}
type PutResponse struct{}

type BatchPutRequest struct {
    Requests []PutRequest
}
type BatchPutResponse struct{}
