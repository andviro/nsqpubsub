package nsqpubsub

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/nsqio/go-nsq"
)

type URLOpener struct{}

type config struct {
	NSQD        string
	NSQLookupDs []string
	Topic       string
	Channel     string
	Config      *nsq.Config `json:"-"`
}

func parseURL(u *url.URL) (config, error) {
	res := config{
		Config: nsq.NewConfig(),
	}
	for k := range u.Query() {
		if err := res.Config.Set(k, u.Query().Get(k)); err != nil {
			return res, fmt.Errorf("setting option %s: %+v", k, err)
		}
	}
	switch u.Scheme {
	case "nsq", "nsqd":
		res.NSQD = u.Host
		res.Topic = strings.Trim(u.Path, "/")
	case "nsqlookupd":
		res.NSQLookupDs = strings.Split(u.Host, ",")
		res.Topic = strings.Trim(u.Path, "/")
		res.Channel = u.Fragment
	default:
		return res, fmt.Errorf("invalid URL scheme: %s", u.Scheme)
	}
	return res, nil
}
