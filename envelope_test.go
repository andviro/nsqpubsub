package nsqpubsub

import "testing"

func TestEnvelope_Encode_Decode(t *testing.T) {
	data, err := packEnvelope(envelope{
		Header: map[string]string{"a": "b"},
		Body:   []byte{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encoded to %d bytes: %s", len(data), data)
	env, err := unpackEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("decoded to %+v", env)
}
