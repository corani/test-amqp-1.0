package add

import (
	"encoding/json"
)

type AddRequest struct {
	A int64 `json:"a"`
	B int64 `json:"b"`
}

type AddResponse struct {
	Ans int64 `json:"ans"`
}

func (req *AddRequest) Encode() ([]byte, error) {
	return json.Marshal(req)
}

func (req *AddRequest) Decode(bs []byte) error {
	return json.Unmarshal(bs, req)
}

func (res *AddResponse) Encode() ([]byte, error) {
	return json.Marshal(res)
}

func (res *AddResponse) Decode(bs []byte) error {
	return json.Unmarshal(bs, res)
}
