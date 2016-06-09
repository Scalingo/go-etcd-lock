package lock

import (
	"fmt"
	ect "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"gopkg.in/errgo.v1"
	"os"
	"sort"
	"time"
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
	client *ect.Client
}

func NewEtcdLocker(client *ect.Client) Locker {
	return &EtcdLocker{client: client}
}

type Lock interface {
	Release() error
}

type EtcdLock struct {
	client *ect.Client
	key    string
	index  uint64
}

func (locker *EtcdLocker) Acquire(key string, ttl uint64) (Lock, error) {
	return locker.acquire(locker.client, key, ttl, false)
}

func (locker *EtcdLocker) WaitAcquire(key string, ttl uint64) (Lock, error) {
	return locker.acquire(locker.client, key, ttl, true)
}

func (locker *EtcdLocker) acquire(client *ect.Client, key string, ttl uint64, wait bool) (Lock, error) {
	hasLock := false
	key = addPrefix(key)
	lock, err := addLockDirChild(client, key)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	kapi := ect.NewKeysAPI(*client)
	ctx := context.Background()
	for !hasLock {
		res, err := kapi.Get(ctx, key, &ect.GetOptions{Recursive: true, Sort: true})
		if err != nil {
			return nil, errgo.Mask(err)
		}

		if len(res.Node.Nodes) > 1 {
			sort.Sort(res.Node.Nodes)
			if res.Node.Nodes[0].CreatedIndex != lock.Node.CreatedIndex {
				if !wait {
					kapi.Delete(ctx, lock.Node.Key, &ect.DeleteOptions{Recursive: false})
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
	_, err = kapi.Set(ctx, lock.Node.Key, lock.Node.Value, &ect.SetOptions{PrevExist: ect.PrevExist, TTL: time.Duration(ttl) * time.Second})
	if err != nil {
		return nil, errgo.Mask(err)
	}

	return &EtcdLock{client, lock.Node.Key, lock.Node.CreatedIndex}, nil
}

func addLockDirChild(client *ect.Client, key string) (*ect.Response, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errgo.Notef(err, "fail to get hostname")
	}

	ctx := context.Background()
	(*client).Sync(ctx)

	kapi := ect.NewKeysAPI(*client)
	return kapi.CreateInOrder(ctx, key, hostname, nil)
}
