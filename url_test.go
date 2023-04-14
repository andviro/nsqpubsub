package nsqpubsub

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"

	"github.com/andviro/goldie"
)

func TestURL_Parse(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range []string{
		"nsq://10.0.0.1:4151,10.0.0.2:4151/topic",
		"nsqd://10.0.0.1:4151,10.0.0.2:4151/topic",
		"nsqlookupd://10.0.0.1:4151,10.0.0.2:4151/topic#channel",
	} {
		u, err := url.Parse(tc)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		cfg, err := parseURL(u)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		jd, _ := json.Marshal(cfg)
		fmt.Fprintf(buf, "%s: %s\n", tc, jd)
	}
	goldie.Assert(t, "url-parse", buf.Bytes())
}
