package lock

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"gopkg.in/errgo.v1"
)

// RW metadata must live outside the public "/etcd-lock/..." namespace so
// internal bookkeeping never collides with user-visible legacy lock keys.
const rwMetadataPrefix = "/go-etcd-lock-rw"

const rwReaderDirectory = "readers"

const rwWriterIntentDirectory = "writer-intents"

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

	client *etcdv3.Client
	// locker carries the RW timing configuration used for upgrade attempts.
	locker *EtcdRWLocker
	// resourceKey is the shared etcd lock namespace for this protected resource.
	resourceKey string
	// lockKey is the current reader entry stored in etcd for this lock instance.
	lockKey string
	// ttl is reused if the read lock is upgraded into a write lock.
	ttl int
	// session owns the current etcd lease backing the reader entry.
	session *concurrency.Session
	// released avoids duplicate cleanup from explicit release and TTL expiry.
	released bool
	// upgrading suppresses normal release while the read lock is being converted.
	upgrading bool
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
	resourceKey := addPrefix(key)
	deadline := time.Now().Add(locker.maxTryLockTimeout)

	for {
		writerPresent, err := locker.hasAnyWriter(resourceKey)
		if err != nil {
			return nil, errgo.Notef(err, "fail to acquire read lock")
		}
		if writerPresent {
			if !wait || time.Now().After(deadline) {
				return nil, &ErrAlreadyLocked{}
			}
			time.Sleep(locker.cooldownTryLockDuration)
			continue
		}

		session, err := concurrency.NewSession(locker.client, concurrency.WithTTL(ttl))
		if err != nil {
			return nil, err
		}

		lock := &EtcdRWLock{
			Mutex:       &sync.Mutex{},
			client:      locker.client,
			locker:      locker,
			resourceKey: resourceKey,
			ttl:         ttl,
			session:     session,
			lockKey:     rwReaderKey(resourceKey, session.Lease()),
		}
		myRev, err := locker.createReaderKey(lock.lockKey, session.Lease())
		if err != nil {
			closeErr := closeRWSession(session)
			return nil, noteAcquireFailure(err, closeErr, "fail to acquire read lock")
		}

		// A reader may proceed alongside other readers, but it must not bypass any
		// earlier legacy writer or RW writer already queued or announced for the
		// same lock key. If such a writer exists, the reader drops its provisional
		// entry and retries later.
		writerAhead, err := locker.hasEarlierWriter(resourceKey, myRev-1)
		if err != nil {
			releaseErr := lock.Release()
			return nil, noteAcquireFailure(err, releaseErr, "fail to acquire read lock")
		}
		if !writerAhead {
			locker.scheduleRelease(lock, ttl)
			return lock, nil
		}
		releaseErr := lock.Release()
		if releaseErr != nil {
			return nil, noteAcquireFailure(&ErrAlreadyLocked{}, releaseErr, "fail to acquire read lock")
		}
		if !wait || time.Now().After(deadline) {
			return nil, &ErrAlreadyLocked{}
		}
		time.Sleep(locker.cooldownTryLockDuration)
	}
}

func (locker *EtcdRWLocker) writeLocker() *EtcdLocker {
	// RW writes intentionally reuse the legacy writer path so old and new writers
	// keep the same writer-vs-writer ordering during rollout.
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

	intentResp, resp, err := locker.writerState(resourceKey, etcdv3.WithMaxCreateRev(maxCreateRev))
	if err != nil {
		return false, err
	}
	if len(intentResp.Kvs) > 0 {
		return true, nil
	}

	return len(resp.Kvs) > 0, nil
}

func (locker *EtcdRWLocker) hasAnyWriter(resourceKey string) (bool, error) {
	intentResp, resp, err := locker.writerState(resourceKey)
	if err != nil {
		return false, err
	}
	if len(intentResp.Kvs) > 0 {
		return true, nil
	}

	return len(resp.Kvs) > 0, nil
}

func (locker *EtcdRWLocker) writerState(resourceKey string, opts ...etcdv3.OpOption) (*etcdv3.GetResponse, *etcdv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), locker.tryLockTimeout)
	defer cancel()

	queueOpts := append([]etcdv3.OpOption{etcdv3.WithPrefix()}, opts...)
	resp, err := locker.client.Get(
		ctx,
		rwQueuePrefix(resourceKey),
		queueOpts...,
	)
	if err != nil {
		return nil, nil, err
	}
	intentOpts := append([]etcdv3.OpOption{etcdv3.WithPrefix(), etcdv3.WithLimit(1)}, opts...)
	intentResp, err := locker.client.Get(
		ctx,
		rwWriterIntentsPrefix(resourceKey),
		intentOpts...,
	)
	if err != nil {
		return nil, nil, err
	}

	return intentResp, resp, nil
}

func (locker *EtcdLocker) hasAnyReader(resourceKey string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), locker.tryLockTimeout)
	defer cancel()

	resp, err := locker.client.Get(ctx, rwReadersPrefix(resourceKey), etcdv3.WithPrefix(), etcdv3.WithLimit(1))
	if err != nil {
		return false, err
	}

	return len(resp.Kvs) > 0, nil
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
	if l.upgrading {
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
	return fmt.Sprintf("%s/%s/%s/", rwMetadataPrefix, rwReaderDirectory, encodeMetadataResource(resourceKey))
}

func rwReaderKey(resourceKey string, leaseID etcdv3.LeaseID) string {
	return fmt.Sprintf("%s%x", rwReadersPrefix(resourceKey), leaseID)
}

func rwWriterIntentsPrefix(resourceKey string) string {
	return fmt.Sprintf("%s/%s/%s/", rwMetadataPrefix, rwWriterIntentDirectory, encodeMetadataResource(resourceKey))
}

func rwWriterIntentKey(resourceKey string, leaseID etcdv3.LeaseID) string {
	return fmt.Sprintf("%s%x", rwWriterIntentsPrefix(resourceKey), leaseID)
}

func noteAcquireFailure(err error, cleanupErr error, message string) error {
	if cleanupErr == nil {
		return errgo.Notef(err, "%s", message)
	}

	return errgo.Notef(err, "%s (additionally failed cleanup: %v)", message, cleanupErr)
}

func encodeMetadataResource(resourceKey string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(resourceKey))
}
