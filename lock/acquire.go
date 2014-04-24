package lock

import (
	"fmt"
	"os"
	"sort"

	"github.com/coreos/go-etcd/etcd"
	"github.com/juju/errgo"
)

type Error struct {
	hostname string
}

func (e *Error) Error() string {
	return fmt.Sprintf("key is already locked by %s", e.hostname)
}

type Lock struct {
	client *etcd.Client
	key    string
	index  uint64
}

func Acquire(client *etcd.Client, key string, ttl uint64) (*Lock, error) {
	key = addPrefix(key)
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errgo.Notef(err, "fail to get hostname")
	}
	client.SyncCluster()

	lock, err := client.AddChild(key, hostname, ttl)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	res, err := client.Get(key, true, true)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	if len(res.Node.Nodes) > 1 {
		sort.Sort(res.Node.Nodes)
		if res.Node.Nodes[0].CreatedIndex != lock.Node.CreatedIndex {
			client.Delete(lock.Node.Key, false)
			return nil, &Error{res.Node.Nodes[0].Value}
		}
	}

	return &Lock{client, lock.Node.Key, lock.Node.CreatedIndex}, nil
}
