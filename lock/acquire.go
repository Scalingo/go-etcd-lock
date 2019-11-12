package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type Error struct{}

func (e *Error) Error() string {
	return fmt.Sprintf("key is already locked")
}

type Locker interface {
	Acquire(key string, ttl int) (Lock, error)
	WaitAcquire(key string, ttl int) (Lock, error)
	Wait(key string) error
}

type EtcdLocker struct {
	client         *etcd.Client
	trylockTimeout time.Duration
}

type EtcdLockerOpt func(locker *EtcdLocker)

func NewEtcdLocker(client *etcd.Client, opts ...EtcdLockerOpt) Locker {
	locker := &EtcdLocker{
		client:         client,
		trylockTimeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(locker)
	}
	return locker
}

func WithTrylockTimeout(timeout time.Duration) EtcdLockerOpt {
	return EtcdLockerOpt(func(locker *EtcdLocker) {
		locker.trylockTimeout = timeout
	})
}

type Lock interface {
	Release() error
}

type EtcdLock struct {
	*sync.Mutex
	mutex *concurrency.Mutex
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
	key = addPrefix(key)
	mutex := concurrency.NewMutex(session, key)

	ctx := context.Background()
	if !wait {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, locker.trylockTimeout)
		defer cancel()
	}

	err = mutex.Lock(ctx)
	if err == context.DeadlineExceeded {
		return nil, &Error{}
	} else if err != nil {
		return nil, err
	}

	lock := &EtcdLock{mutex: mutex, Mutex: &sync.Mutex{}}

	go func() {
		time.AfterFunc(time.Duration(ttl)*time.Second, func() {
			lock.Release()
		})
	}()

	return lock, nil
}
