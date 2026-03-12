package lock

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"gopkg.in/errgo.v1"
)

// RW readers live under the same legacy queue prefix but in their own
// subdirectory so old writers still see them as earlier queue entries.
const rwReaderDirectory = "__rwlock-readers"

type RWLocker interface {
	AcquireRead(key string, ttl int) (Lock, error)
	WaitAcquireRead(key string, ttl int) (Lock, error)
	AcquireWrite(key string, ttl int) (Lock, error)
	WaitAcquireWrite(key string, ttl int) (Lock, error)
	Wait(key string) error
}

type EtcdRWLocker struct {
	client *etcdv3.Client
	// tryLockTimeout is the timeout duration for one attempt to create the lock.
	tryLockTimeout time.Duration
	// maxTryLockTimeout is the maximal time the wait variants can block.
	maxTryLockTimeout time.Duration
	// cooldownTryLockDuration is the pause between retries while waiting.
	cooldownTryLockDuration time.Duration
}

type EtcdRWLock struct {
	*sync.Mutex

	client   *etcdv3.Client
	session  *concurrency.Session
	lockKey  string
	released bool
}

func NewEtcdRWLocker(client *etcdv3.Client, opts ...EtcdLockerOpt) RWLocker {
	base := &EtcdLocker{
		client:                  client,
		tryLockTimeout:          30 * time.Second,
		maxTryLockTimeout:       2 * time.Minute,
		cooldownTryLockDuration: time.Second,
	}
	for _, opt := range opts {
		opt(base)
	}

	return &EtcdRWLocker{
		client:                  client,
		tryLockTimeout:          base.tryLockTimeout,
		maxTryLockTimeout:       base.maxTryLockTimeout,
		cooldownTryLockDuration: base.cooldownTryLockDuration,
	}
}

func (locker *EtcdRWLocker) AcquireRead(key string, ttl int) (Lock, error) {
	return locker.acquireRead(key, ttl, false)
}

func (locker *EtcdRWLocker) WaitAcquireRead(key string, ttl int) (Lock, error) {
	return locker.acquireRead(key, ttl, true)
}

func (locker *EtcdRWLocker) AcquireWrite(key string, ttl int) (Lock, error) {
	return locker.writeLocker().Acquire(key, ttl)
}

func (locker *EtcdRWLocker) WaitAcquireWrite(key string, ttl int) (Lock, error) {
	return locker.writeLocker().WaitAcquire(key, ttl)
}

func (locker *EtcdRWLocker) Wait(key string) error {
	return locker.writeLocker().Wait(key)
}

func (locker *EtcdRWLocker) acquireRead(key string, ttl int, wait bool) (Lock, error) {
	session, err := concurrency.NewSession(locker.client, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}

	resourceKey := addPrefix(key)
	lock := &EtcdRWLock{
		Mutex:   &sync.Mutex{},
		client:  locker.client,
		session: session,
		lockKey: rwReaderKey(resourceKey, session.Lease()),
	}
	deadline := time.Now().Add(locker.maxTryLockTimeout)
	myRev, err := locker.createReaderKey(lock.lockKey, session.Lease())
	if err != nil {
		closeErr := closeRWSession(session)
		return nil, noteAcquireFailure(err, closeErr, "fail to acquire read lock")
	}

	for {
		// A reader may proceed alongside other readers, but it must not bypass any
		// earlier legacy writer or RW writer already queued on the same lock key.
		writerAhead, err := locker.hasEarlierWriter(resourceKey, myRev-1)
		if err != nil {
			releaseErr := lock.Release()
			return nil, noteAcquireFailure(err, releaseErr, "fail to acquire read lock")
		}
		if !writerAhead {
			locker.scheduleRelease(lock, ttl)
			return lock, nil
		}
		if !wait || time.Now().After(deadline) {
			releaseErr := lock.Release()
			if releaseErr != nil {
				return nil, releaseErr
			}
			return nil, &ErrAlreadyLocked{}
		}
		time.Sleep(locker.cooldownTryLockDuration)
	}
}

func (locker *EtcdRWLocker) writeLocker() *EtcdLocker {
	// RW writes intentionally reuse the legacy lock path so old and new writers
	// compete on exactly the same etcd entries during rollout.
	return &EtcdLocker{
		client:                  locker.client,
		tryLockTimeout:          locker.tryLockTimeout,
		maxTryLockTimeout:       locker.maxTryLockTimeout,
		cooldownTryLockDuration: locker.cooldownTryLockDuration,
	}
}

func (locker *EtcdRWLocker) createReaderKey(lockKey string, leaseID etcdv3.LeaseID) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), locker.tryLockTimeout)
	defer cancel()

	resp, err := locker.client.Txn(ctx).
		If(etcdv3.Compare(etcdv3.CreateRevision(lockKey), "=", 0)).
		Then(etcdv3.OpPut(lockKey, "", etcdv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, errgo.Newf("reader key %q already exists", lockKey)
	}

	return resp.Header.Revision, nil
}

func (locker *EtcdRWLocker) hasEarlierWriter(resourceKey string, maxCreateRev int64) (bool, error) {
	if maxCreateRev <= 0 {
		return false, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), locker.tryLockTimeout)
	defer cancel()

	resp, err := locker.client.Get(
		ctx,
		rwQueuePrefix(resourceKey),
		etcdv3.WithPrefix(),
		etcdv3.WithMaxCreateRev(maxCreateRev),
	)
	if err != nil {
		return false, err
	}

	readerPrefix := rwReadersPrefix(resourceKey)
	// Anything in the shared queue that is not under the reader subtree is a
	// writer entry, either from the legacy locker or from RW AcquireWrite.
	for _, kv := range resp.Kvs {
		if !strings.HasPrefix(string(kv.Key), readerPrefix) {
			return true, nil
		}
	}
	return false, nil
}

func (locker *EtcdRWLocker) scheduleRelease(lock *EtcdRWLock, ttl int) {
	time.AfterFunc(time.Duration(ttl)*time.Second, func() {
		_ = lock.Release()
	})
}

func (l *EtcdRWLock) Release() error {
	if l == nil {
		return errgo.New("nil lock")
	}

	l.Lock()
	defer l.Unlock()

	if l.released {
		return nil
	}

	l.released = true

	_, err := l.client.Delete(context.Background(), l.lockKey)
	closeErr := closeRWSession(l.session)

	if err != nil {
		return err
	}
	return closeErr
}

func closeRWSession(session *concurrency.Session) error {
	if session == nil {
		return nil
	}

	err := session.Close()
	if err == nil || err == rpctypes.ErrLeaseNotFound {
		return nil
	}
	return err
}

func rwQueuePrefix(resourceKey string) string {
	return resourceKey + "/"
}

func rwReadersPrefix(resourceKey string) string {
	return fmt.Sprintf("%s%s/", rwQueuePrefix(resourceKey), rwReaderDirectory)
}

func rwReaderKey(resourceKey string, leaseID etcdv3.LeaseID) string {
	return fmt.Sprintf("%s%x", rwReadersPrefix(resourceKey), leaseID)
}

func noteAcquireFailure(err error, cleanupErr error, message string) error {
	if cleanupErr == nil {
		return errgo.Notef(err, "%s", message)
	}

	return errgo.Notef(err, "%s (additionally failed cleanup: %v)", message, cleanupErr)
}
