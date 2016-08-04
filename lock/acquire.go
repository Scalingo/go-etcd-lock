package lock

import (
	"fmt"
	"os"
	"sort"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
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
	Wait(key string) error
}

type EtcdLocker struct {
	kapi   etcd.KeysAPI
	client etcd.Client
}

func NewEtcdLocker(client etcd.Client) Locker {
	return &EtcdLocker{kapi: etcd.NewKeysAPI(client), client: client}
}

type Lock interface {
	Release() error
}

type EtcdLock struct {
	kapi  etcd.KeysAPI
	key   string
	index uint64
}

func (locker *EtcdLocker) Acquire(key string, ttl uint64) (Lock, error) {
	return locker.acquire(locker.kapi, key, ttl, false)
}

func (locker *EtcdLocker) WaitAcquire(key string, ttl uint64) (Lock, error) {
	return locker.acquire(locker.kapi, key, ttl, true)
}

func (locker *EtcdLocker) acquire(kapi etcd.KeysAPI, key string, ttl uint64, wait bool) (Lock, error) {
	hasLock := false
	key = addPrefix(key)
	lock, err := addLockDirChild(locker.client, kapi, key)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	for !hasLock {
		res, err := kapi.Get(context.Background(), key, &etcd.GetOptions{Recursive: true, Sort: true})
		if err != nil {
			return nil, errgo.Mask(err)
		}

		if len(res.Node.Nodes) > 1 {
			sort.Sort(res.Node.Nodes)
			if res.Node.Nodes[0].CreatedIndex != lock.Node.CreatedIndex {
				if !wait {
					kapi.Delete(context.Background(), lock.Node.Key, &etcd.DeleteOptions{})
					return nil, &Error{res.Node.Nodes[0].Value}
				} else {
					err = locker.Wait(lock.Node.Key)
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
	_, err = kapi.Set(context.Background(), lock.Node.Key, lock.Node.Value, &etcd.SetOptions{TTL: time.Duration(ttl) * time.Second})
	if err != nil {
		return nil, errgo.Mask(err)
	}

	return &EtcdLock{kapi, lock.Node.Key, lock.Node.CreatedIndex}, nil
}

func addLockDirChild(client etcd.Client, kapi etcd.KeysAPI, key string) (*etcd.Response, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errgo.Notef(err, "fail to get hostname")
	}

	client.Sync(context.Background())

	return kapi.CreateInOrder(context.Background(), key, hostname, &etcd.CreateInOrderOptions{})
}
