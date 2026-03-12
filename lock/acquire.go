package lock

import (
	"context"
	"sync"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"gopkg.in/errgo.v1"
)

type ErrAlreadyLocked struct{}

func (e *ErrAlreadyLocked) Error() string {
	return "key is already locked"
}

type Locker interface {
	Acquire(key string, ttl int) (Lock, error)
	WaitAcquire(key string, ttl int) (Lock, error)
	Wait(key string) error
}

type EtcdLocker struct {
	client *etcdv3.Client
	// tryLockTimeout is the timeout duration for one attempt to create the lock
	// When executing Acquire(), it will return a failed to lock error after this
	// duration
	tryLockTimeout time.Duration
	// maxTryLockTimeout is the maximal timeout duration that WaitAcquire can wait
	// to avoid it from waiting indefinitely.
	maxTryLockTimeout time.Duration
	// cooldownTryLockDuration is the duration between attempt to take the lock when
	// waiting to take the lock
	cooldownTryLockDuration time.Duration
}

type EtcdLockerOpt func(locker *EtcdLocker)

func NewEtcdLocker(client *etcdv3.Client, opts ...EtcdLockerOpt) Locker {
	locker := &EtcdLocker{
		client:                  client,
		tryLockTimeout:          30 * time.Second,
		maxTryLockTimeout:       2 * time.Minute,
		cooldownTryLockDuration: time.Second,
	}
	for _, opt := range opts {
		opt(locker)
	}
	return locker
}

func WithTryLockTimeout(timeout time.Duration) EtcdLockerOpt {
	return EtcdLockerOpt(func(locker *EtcdLocker) {
		locker.tryLockTimeout = timeout
	})
}

func WithMaxTryLockTimeout(timeout time.Duration) EtcdLockerOpt {
	return EtcdLockerOpt(func(locker *EtcdLocker) {
		locker.maxTryLockTimeout = timeout
	})
}

func WithCooldownTryLockDuration(timeout time.Duration) EtcdLockerOpt {
	return EtcdLockerOpt(func(locker *EtcdLocker) {
		locker.cooldownTryLockDuration = timeout
	})
}

type Lock interface {
	Release() error
}

type EtcdLock struct {
	*sync.Mutex

	mutex   *concurrency.Mutex
	session *concurrency.Session
}

func (locker *EtcdLocker) Acquire(key string, ttl int) (Lock, error) {
	return locker.acquire(key, ttl, false)
}

func (locker *EtcdLocker) WaitAcquire(key string, ttl int) (Lock, error) {
	return locker.acquire(key, ttl, true)
}

func (locker *EtcdLocker) acquire(key string, ttl int, wait bool) (Lock, error) {
	// A Session is a GRPC connection to ETCD API v3, the connection should be
	// closed to release resources.
	session, err := concurrency.NewSession(locker.client, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}

	key = addPrefix(key)
	mutex := concurrency.NewMutex(session, key)

	var lockErr error
	if wait {
		lockErr = locker.waitLock(mutex)
	} else {
		lockErr = locker.tryLock(mutex)
	}
	if lockErr != nil {
		session.Close()
		if lockErr == context.DeadlineExceeded {
			return nil, &ErrAlreadyLocked{}
		}
		return nil, errgo.Notef(lockErr, "fail to acquire lock")
	}

	lock := &EtcdLock{mutex: mutex, Mutex: &sync.Mutex{}, session: session}

	go func() {
		time.AfterFunc(time.Duration(ttl)*time.Second, func() {
			lock.Release()
		})
	}()

	return lock, nil
}

func (locker *EtcdLocker) tryLock(mutex *concurrency.Mutex) error {
	ctx, cancel := context.WithTimeout(context.Background(), locker.tryLockTimeout)
	defer cancel()
	return mutex.Lock(ctx)
}

func (locker *EtcdLocker) waitLock(mutex *concurrency.Mutex) error {
	deadline := time.Now().Add(locker.maxTryLockTimeout)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}

		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		err := mutex.Lock(ctx)
		cancel()
		if err == nil || err == context.DeadlineExceeded {
			return err
		}

		time.Sleep(locker.cooldownTryLockDuration)
	}
}
