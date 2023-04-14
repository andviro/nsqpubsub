package nsqpubsub

import (
	"bytes"
	"encoding/gob"
)

type envelope struct {
	Header map[string]string
	Body   []byte
}

func packEnvelope(env envelope) ([]byte, error) {
	dest := new(bytes.Buffer)
	enc := gob.NewEncoder(dest)
	if err := enc.Encode(env); err != nil {
		return nil, err
	}
	return dest.Bytes(), nil
}

func unpackEnvelope(src []byte) (envelope, error) {
	dec := gob.NewDecoder(bytes.NewReader(src))
	var res envelope
	if err := dec.Decode(&res); err != nil {
		return res, err
	}
	return res, nil
}
