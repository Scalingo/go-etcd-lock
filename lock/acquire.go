package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type ErrAlreadyLocked struct{}

func (e *ErrAlreadyLocked) Error() string {
	return fmt.Sprintf("key is already locked")
}

type Locker interface {
	Acquire(key string, ttl int) (Lock, error)
	WaitAcquire(key string, ttl int) (Lock, error)
	Wait(key string) error
}

type EtcdLocker struct {
	client *etcd.Client
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

func NewEtcdLocker(client *etcd.Client, opts ...EtcdLockerOpt) Locker {
	locker := &EtcdLocker{
		client:                  client,
		tryLockTimeout:          10 * time.Second,
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
	timeout := time.NewTimer(locker.maxTryLockTimeout)

	for {
		// If we've wait more than the maxTryLockTimeout, we stop waiting and
		// consider the lock already taken
		select {
		case <-timeout.C:
			session.Close()
			return nil, &ErrAlreadyLocked{}
		default:
		}

		// Otherwise we try locking:
		// * If the attempt fails and we're still waiting, we retry the operation after a short cooldown
		// * if the attempt fails and we're not waiting, the lock is already taken
		err := locker.tryLock(mutex)

		shouldWait := wait && err == context.DeadlineExceeded
		shouldRetry := shouldWait || (err != nil && err != context.DeadlineExceeded)
		if shouldRetry {
			time.Sleep(locker.cooldownTryLockDuration)
			continue
		} else if err == context.DeadlineExceeded {
			session.Close()
			return nil, &ErrAlreadyLocked{}
		} else {
			break
		}
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
