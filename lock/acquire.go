package lock

import (
	"fmt"
	"os"
	"sort"

	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/errgo.v1"
)

type Error struct {
	hostname string
}

func (e *Error) Error() string {
	return fmt.Sprintf("key is already locked by %s", e.hostname)
}

type Locker interface {
	Acquire(key string, ttl uint64) (Lock, error)
	WaitAcquire(key string, ttl uint64) (Lock, error)
}

type EtcdLocker struct {
	client *etcd.Client
}

func NewEtcdLocker(client *etcd.Client) Locker {
	return &EtcdLocker{client: client}
}

type Lock interface {
	Release() error
}

type EtcdLock struct {
	client *etcd.Client
	key    string
	index  uint64
}

func (locker *EtcdLocker) Acquire(key string, ttl uint64) (Lock, error) {
	return acquire(locker.client, key, ttl, false)
}

func (locker *EtcdLocker) WaitAcquire(key string, ttl uint64) (Lock, error) {
	return acquire(locker.client, key, ttl, true)
}

func acquire(client *etcd.Client, key string, ttl uint64, wait bool) (Lock, error) {
	hasLock := false
	key = addPrefix(key)
	lock, err := addLockDirChild(client, key)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	for !hasLock {
		res, err := client.Get(key, true, true)
		if err != nil {
			return nil, errgo.Mask(err)
		}

		if len(res.Node.Nodes) > 1 {
			sort.Sort(res.Node.Nodes)
			if res.Node.Nodes[0].CreatedIndex != lock.Node.CreatedIndex {
				if !wait {
					client.Delete(lock.Node.Key, false)
					return nil, &Error{res.Node.Nodes[0].Value}
				} else {
					err = Wait(client, lock.Node.Key)
					if err != nil {
						return nil, errgo.Mask(err)
					}
				}
			} else {
				// if the first index is the current one, it's our turn to lock the key
				hasLock = true
			}
		} else {
			// If there are only 1 node, it's our, lock is acquired
			hasLock = true
		}
	}

	// If we get the lock, set the ttl and return it
	_, err = client.Update(lock.Node.Key, lock.Node.Value, ttl)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	return &EtcdLock{client, lock.Node.Key, lock.Node.CreatedIndex}, nil
}

func addLockDirChild(client *etcd.Client, key string) (*etcd.Response, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errgo.Notef(err, "fail to get hostname")
	}
	client.SyncCluster()

	return client.AddChild(key, hostname, 0)
}
