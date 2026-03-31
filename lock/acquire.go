package lock

import (
	"context"
	"sync"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/Scalingo/go-utils/errors/v3"
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

	client    *etcdv3.Client
	mutex     *concurrency.Mutex
	session   *concurrency.Session
	intentKey string
}

func (locker *EtcdLocker) Acquire(key string, ttl int) (Lock, error) {
	return locker.acquire(key, ttl, false)
}

func (locker *EtcdLocker) WaitAcquire(key string, ttl int) (Lock, error) {
	return locker.acquire(key, ttl, true)
}

func (locker *EtcdLocker) acquire(key string, ttl int, wait bool) (Lock, error) {
	ctx := context.Background()

	// A Session is a GRPC connection to ETCD API v3, the connection should be
	// closed to release resources.
	session, err := concurrency.NewSession(locker.client, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, errors.Wrap(ctx, err, "create lock session")
	}

	key = addPrefix(key)
	mutex := concurrency.NewMutex(session, key)

	timeout := time.NewTimer(locker.maxTryLockTimeout)
	defer timeout.Stop()
	deadline := time.Now().Add(locker.maxTryLockTimeout)

	var tryLockErr error
	intentCreated := !wait
	intentKey := rwWriterIntentKey(key, session.Lease())
	for {
		// If we've wait more than the maxTryLockTimeout, we stop waiting and
		// consider the lock already taken.
		select {
		case <-timeout.C:
			session.Close()
			if tryLockErr == context.DeadlineExceeded {
				return nil, &ErrAlreadyLocked{}
			}
			return nil, errors.Wrap(ctx, tryLockErr, "acquire lock")
		default:
		}

		if !intentCreated {
			tryLockErr = locker.createWriterIntent(ctx, intentKey, session.Lease())
			if tryLockErr == nil {
				intentCreated = true
			} else {
				time.Sleep(locker.cooldownTryLockDuration)
				continue
			}
		}

		// Otherwise we try locking:
		// * If the attempt fails and we're still waiting, we retry the operation after a short cooldown
		// * if the attempt fails and we're not waiting, the lock is already taken
		// * if the attempt succeeded, keep on
		tryLockErr = locker.tryLock(ctx, mutex)

		shouldWait := wait && tryLockErr == context.DeadlineExceeded
		shouldRetry := shouldWait || (tryLockErr != nil && tryLockErr != context.DeadlineExceeded)
		if shouldRetry {
			time.Sleep(locker.cooldownTryLockDuration)
			continue
		} else if tryLockErr == context.DeadlineExceeded {
			session.Close()
			return nil, &ErrAlreadyLocked{}
		} else {
			break
		}
	}

	lock := &EtcdLock{
		Mutex:   &sync.Mutex{},
		client:  locker.client,
		mutex:   mutex,
		session: session,
	}
	if intentCreated {
		lock.intentKey = intentKey
	}
	err = locker.waitForReaders(ctx, key, wait, deadline)
	if err != nil {
		releaseErr := lock.Release()
		var alreadyLocked *ErrAlreadyLocked
		if errors.As(err, &alreadyLocked) {
			if releaseErr != nil {
				return nil, noteAcquireFailure(&ErrAlreadyLocked{}, releaseErr, "acquire lock")
			}
			return nil, errors.Wrap(ctx, err, "acquire lock")
		}
		return nil, noteAcquireFailure(err, releaseErr, "acquire lock")
	}

	scheduleRelease(lock, ttl)

	return lock, nil
}

func (locker *EtcdLocker) tryLock(ctx context.Context, mutex *concurrency.Mutex) error {
	ctx, cancel := context.WithTimeout(ctx, locker.tryLockTimeout)
	defer cancel()
	return mutex.Lock(ctx)
}

func (locker *EtcdLocker) waitForReaders(ctx context.Context, resourceKey string, wait bool, deadline time.Time) error {
	for {
		readersPresent, err := locker.hasAnyReader(resourceKey)
		if err != nil {
			return errors.Wrap(ctx, err, "check current readers")
		}
		if !readersPresent {
			return nil
		}
		if !wait || time.Now().After(deadline) {
			return &ErrAlreadyLocked{}
		}
		time.Sleep(locker.cooldownTryLockDuration)
	}
}

func (locker *EtcdLocker) createWriterIntent(ctx context.Context, intentKey string, leaseID etcdv3.LeaseID) error {
	ctx, cancel := context.WithTimeout(ctx, locker.tryLockTimeout)
	defer cancel()

	resp, err := locker.client.Txn(ctx).
		If(etcdv3.Compare(etcdv3.CreateRevision(intentKey), "=", 0)).
		Then(etcdv3.OpPut(intentKey, "", etcdv3.WithLease(leaseID))).
		Else(etcdv3.OpGet(intentKey)).
		Commit()
	if err != nil {
		return errors.Wrap(ctx, err, "create writer intent")
	}
	if !resp.Succeeded && len(resp.Responses[0].GetResponseRange().Kvs) == 0 {
		return errors.Newf(ctx, "writer intent key %q already exists", intentKey)
	}

	return nil
}
