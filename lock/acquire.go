package lock

import (
	"fmt"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

type Error struct{}

func (e *Error) Error() string {
	return fmt.Sprintf("key is already locked")
}

type Locker interface {
	Acquire(key string, ttl int) (Lock, error)
	WaitAcquire(key string, ttl int) (Lock, error)
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
	mutex sync.Locker
}

func (locker *EtcdLocker) Acquire(key string, ttl int) (Lock, error) {
	return locker.acquire(key, ttl, false)
}

func (locker *EtcdLocker) WaitAcquire(key string, ttl int) (Lock, error) {
	return locker.acquire(key, ttl, true)
}

func (locker *EtcdLocker) acquire(key string, ttl int, wait bool) (Lock, error) {
	session, err := concurrency.NewSession(locker.client, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	lock := concurrency.NewLocker(session, key)

	if wait {
		lock.Lock()
		return &EtcdLock{mutex: lock}, nil
	} else {
		timer := time.NewTimer(10 * time.Second)
		gotLock := make(chan struct{})
		go func() {
			lock.Lock()
			close(gotLock)
		}()
		select {
		case <-gotLock:
			return &EtcdLock{mutex: lock}, nil
		case <-timer.C:
			return nil, &Error{}
		}
	}
}
