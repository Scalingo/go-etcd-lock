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

	"github.com/Scalingo/go-utils/errors/v3"
)

// RW metadata must live outside the public "/etcd-lock/..." namespace so
// internal bookkeeping never collides with user-visible legacy lock keys.
const rwMetadataPrefix = "/go-etcd-lock-rw"

const rwReaderDirectory = "readers"

const rwWriterIntentDirectory = "writer-intents"

type RWLocker interface {
	AcquireRead(key string, ttl int) (Lock, error)
	AcquireReadWithContext(ctx context.Context, key string, ttl int) (Lock, error)
	WaitAcquireRead(key string, ttl int) (Lock, error)
	WaitAcquireReadWithContext(ctx context.Context, key string, ttl int) (Lock, error)
	AcquireWrite(key string, ttl int) (Lock, error)
	AcquireWriteWithContext(ctx context.Context, key string, ttl int) (Lock, error)
	WaitAcquireWrite(key string, ttl int) (Lock, error)
	WaitAcquireWriteWithContext(ctx context.Context, key string, ttl int) (Lock, error)
	Wait(key string) error
	WaitWithContext(ctx context.Context, key string) error
}

type EtcdRWLocker struct {
	writer *EtcdLocker
}

type EtcdRWLock struct {
	*sync.Mutex

	client   *etcdv3.Client
	session  *concurrency.Session
	lockKey  string
	released bool
}

func NewEtcdRWLocker(client *etcdv3.Client, opts ...EtcdLockerOpt) RWLocker {
	writer := &EtcdLocker{
		client:                  client,
		tryLockTimeout:          30 * time.Second,
		maxTryLockTimeout:       2 * time.Minute,
		cooldownTryLockDuration: time.Second,
	}
	for _, opt := range opts {
		opt(writer)
	}

	return &EtcdRWLocker{
		writer: writer,
	}
}

func (locker *EtcdRWLocker) AcquireRead(key string, ttl int) (Lock, error) {
	return locker.acquireRead(context.Background(), key, ttl, false)
}

func (locker *EtcdRWLocker) AcquireReadWithContext(ctx context.Context, key string, ttl int) (Lock, error) {
	return locker.acquireRead(ctx, key, ttl, false)
}

func (locker *EtcdRWLocker) WaitAcquireRead(key string, ttl int) (Lock, error) {
	return locker.acquireRead(context.Background(), key, ttl, true)
}

func (locker *EtcdRWLocker) WaitAcquireReadWithContext(ctx context.Context, key string, ttl int) (Lock, error) {
	return locker.acquireRead(ctx, key, ttl, true)
}

func (locker *EtcdRWLocker) AcquireWrite(key string, ttl int) (Lock, error) {
	return locker.AcquireWriteWithContext(context.Background(), key, ttl)
}

func (locker *EtcdRWLocker) AcquireWriteWithContext(ctx context.Context, key string, ttl int) (Lock, error) {
	return locker.writer.AcquireWithContext(ctx, key, ttl)
}

func (locker *EtcdRWLocker) WaitAcquireWrite(key string, ttl int) (Lock, error) {
	return locker.WaitAcquireWriteWithContext(context.Background(), key, ttl)
}

func (locker *EtcdRWLocker) WaitAcquireWriteWithContext(ctx context.Context, key string, ttl int) (Lock, error) {
	return locker.writer.WaitAcquireWithContext(ctx, key, ttl)
}

func (locker *EtcdRWLocker) Wait(key string) error {
	return locker.WaitWithContext(context.Background(), key)
}

func (locker *EtcdRWLocker) WaitWithContext(ctx context.Context, key string) error {
	return locker.writer.WaitWithContext(ctx, key)
}

// acquireRead coordinates with both legacy writers and RW writers:
// 1. check whether a writer is already active or queued for this resource;
// 2. create a leased reader entry in the private RW metadata tree;
// 3. compare that reader revision with existing writers;
// 4. if an earlier writer exists, drop the provisional reader entry and retry.
//
// Readers can run concurrently with each other, but they must never jump ahead
// of a writer that was already visible in the shared ordering.
func (locker *EtcdRWLocker) acquireRead(ctx context.Context, key string, ttl int, wait bool) (Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(ctx, err, "check context")
	}

	resourceKey := addPrefix(key)
	deadline := time.Now().Add(locker.writer.maxTryLockTimeout)

	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrap(ctx, err, "check context")
		}

		writerPresent, err := locker.hasAnyWriter(ctx, resourceKey)
		if err != nil {
			return nil, errors.Wrap(ctx, err, "check current write lock")
		}
		if writerPresent {
			if !wait || time.Now().After(deadline) {
				return nil, &ErrAlreadyLocked{}
			}
			if err := locker.writer.waitForRetry(ctx); err != nil {
				return nil, errors.Wrap(ctx, err, "wait for retry")
			}
			continue
		}

		session, err := concurrency.NewSession(locker.writer.client, concurrency.WithTTL(ttl))
		if err != nil {
			return nil, errors.Wrap(ctx, err, "create read lock session")
		}

		lock := &EtcdRWLock{
			Mutex:   &sync.Mutex{},
			client:  locker.writer.client,
			session: session,
			lockKey: rwReaderKey(resourceKey, session.Lease()),
		}
		myRev, err := locker.createReaderKey(ctx, lock.lockKey, session.Lease())
		if err != nil {
			closeErr := closeRWSession(session)
			if closeErr != nil {
				return nil, errors.Wrapf(ctx, err, "acquire read lock: create reader key (cleanup: %v)", closeErr)
			}
			return nil, errors.Wrap(ctx, err, "acquire read lock: create reader key")
		}

		// A reader may proceed alongside other readers, but it must not bypass any
		// earlier legacy writer or RW writer already queued or announced for the
		// same lock key. If such a writer exists, the reader drops its provisional
		// entry and retries later.
		writerAhead, err := locker.hasEarlierWriter(ctx, resourceKey, myRev-1)
		if err != nil {
			releaseErr := lock.Release()
			if releaseErr != nil {
				return nil, errors.Wrapf(ctx, err, "acquire read lock: check earlier writer (cleanup: %v)", releaseErr)
			}
			return nil, errors.Wrap(ctx, err, "acquire read lock: check earlier writer")
		}
		if !writerAhead {
			scheduleRelease(lock, ttl)
			return lock, nil
		}
		releaseErr := lock.Release()
		if releaseErr != nil {
			return nil, errors.Wrapf(ctx, &ErrAlreadyLocked{}, "release lock (cleanup: %v)", releaseErr)
		}
		if !wait || time.Now().After(deadline) {
			return nil, &ErrAlreadyLocked{}
		}
		if err := locker.writer.waitForRetry(ctx); err != nil {
			return nil, errors.Wrap(ctx, err, "wait for retry")
		}
	}
}

func (locker *EtcdRWLocker) createReaderKey(ctx context.Context, lockKey string, leaseID etcdv3.LeaseID) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, locker.writer.tryLockTimeout)
	defer cancel()

	resp, err := locker.writer.client.Txn(ctx).
		If(etcdv3.Compare(etcdv3.CreateRevision(lockKey), "=", 0)).
		Then(etcdv3.OpPut(lockKey, "", etcdv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		return 0, errors.Wrap(ctx, err, "create reader key")
	}
	if !resp.Succeeded {
		return 0, errors.Newf(ctx, "reader key %q already exists", lockKey)
	}

	return resp.Header.Revision, nil
}

func (locker *EtcdRWLocker) hasEarlierWriter(ctx context.Context, resourceKey string, maxCreateRev int64) (bool, error) {
	if maxCreateRev <= 0 {
		return false, nil
	}

	return locker.hasWriter(ctx, resourceKey, etcdv3.WithMaxCreateRev(maxCreateRev))
}

func (locker *EtcdRWLocker) hasAnyWriter(ctx context.Context, resourceKey string) (bool, error) {
	return locker.hasWriter(ctx, resourceKey)
}

// Writers are visible in two places:
//   - the public legacy queue under "<resource>/", shared by legacy and RW writers;
//   - the private writer-intent tree, used by waiting legacy writers before they
//     have fully acquired the mutex.
func (locker *EtcdRWLocker) hasWriter(ctx context.Context, resourceKey string, opts ...etcdv3.OpOption) (bool, error) {
	intentResp, resp, err := locker.writerState(ctx, resourceKey, opts...)
	if err != nil {
		return false, err
	}
	if len(intentResp.Kvs) > 0 {
		return true, nil
	}

	return len(resp.Kvs) > 0, nil
}

func (locker *EtcdRWLocker) writerState(ctx context.Context, resourceKey string, opts ...etcdv3.OpOption) (*etcdv3.GetResponse, *etcdv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, locker.writer.tryLockTimeout)
	defer cancel()

	queueOpts := append([]etcdv3.OpOption{etcdv3.WithPrefix()}, opts...)
	resp, err := locker.writer.client.Get(
		ctx,
		rwQueuePrefix(resourceKey),
		queueOpts...,
	)
	if err != nil {
		return nil, nil, errors.Wrap(ctx, err, "list queued writers")
	}
	intentOpts := append([]etcdv3.OpOption{etcdv3.WithPrefix(), etcdv3.WithLimit(1)}, opts...)
	intentResp, err := locker.writer.client.Get(
		ctx,
		rwWriterIntentsPrefix(resourceKey),
		intentOpts...,
	)
	if err != nil {
		return nil, nil, errors.Wrap(ctx, err, "list writer intents")
	}

	return intentResp, resp, nil
}

// Upgraded legacy writers consult the private reader tree after taking the
// legacy mutex, so active RW readers still block writes until they drain.
// Older binaries do not perform that extra check: they only observe the public
// legacy queue and can therefore write while an RW reader is still active.
// Mixed-version rollout is only safe once every writer path uses this code.
func (locker *EtcdLocker) hasAnyReader(ctx context.Context, resourceKey string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, locker.tryLockTimeout)
	defer cancel()

	resp, err := locker.client.Get(ctx, rwReadersPrefix(resourceKey), etcdv3.WithPrefix(), etcdv3.WithLimit(1))
	if err != nil {
		return false, errors.Wrap(ctx, err, "list active readers")
	}

	return len(resp.Kvs) > 0, nil
}

func (l *EtcdRWLock) Release() error {
	if l == nil {
		panic("nil rw lock")
	}

	l.Lock()
	defer l.Unlock()

	if l.released {
		return nil
	}

	_, err := l.client.Delete(context.Background(), l.lockKey)
	if err != nil {
		return errors.Wrap(context.Background(), err, "delete read lock key")
	}
	err = closeRWSession(l.session)
	if err != nil {
		return err
	}

	l.released = true
	return nil
}

func closeRWSession(session *concurrency.Session) error {
	if session == nil {
		return nil
	}

	err := session.Close()
	if err == nil || err == rpctypes.ErrLeaseNotFound {
		return nil
	}
	return errors.Wrap(context.Background(), err, "close rw lock session")
}

// Key layout for a resourceKey like "/etcd-lock/my-lock":
// - public writer queue prefix: "/etcd-lock/my-lock/"
// - private readers prefix: "/go-etcd-lock-rw/readers/<base64('/etcd-lock/my-lock')>/"
// - private reader key: "/go-etcd-lock-rw/readers/<base64('/etcd-lock/my-lock')>/<lease>"
// - private writer intents prefix: "/go-etcd-lock-rw/writer-intents/<base64('/etcd-lock/my-lock')>/"
// - private writer intent key: "/go-etcd-lock-rw/writer-intents/<base64('/etcd-lock/my-lock')>/<lease>"
//
// The helper names follow that split:
// - "...Prefix" returns the directory-like prefix ending with "/"
// - "...Key" appends one lease id under that prefix
func rwQueuePrefix(resourceKey string) string {
	return resourceKey + "/"
}

// rwReadersPrefix returns the private directory that contains every active
// reader entry for one resource.
func rwReadersPrefix(resourceKey string) string {
	return rwMetadataResourcePrefix(resourceKey, rwReaderDirectory)
}

// rwReaderKey returns the exact etcd key for one reader lease under the reader
// directory of the resource.
func rwReaderKey(resourceKey string, leaseID etcdv3.LeaseID) string {
	return fmt.Sprintf("%s%x", rwReadersPrefix(resourceKey), leaseID)
}

// rwWriterIntentsPrefix returns the private directory that contains waiting
// writer intents for one resource.
func rwWriterIntentsPrefix(resourceKey string) string {
	return rwMetadataResourcePrefix(resourceKey, rwWriterIntentDirectory)
}

// rwWriterIntentKey returns the exact etcd key for one waiting writer lease
// under the writer-intent directory of the resource.
func rwWriterIntentKey(resourceKey string, leaseID etcdv3.LeaseID) string {
	return fmt.Sprintf("%s%x", rwWriterIntentsPrefix(resourceKey), leaseID)
}

func rwMetadataResourcePrefix(resourceKey string, kind string) string {
	// A raw resource key cannot be embedded directly in a metadata prefix tree:
	// "/foo" would become a prefix of "/foo/bar". Encoding keeps each resource
	// in a single opaque path segment, so prefix scans stay scoped to one lock.
	// Example: "/etcd-lock/my-lock" becomes one "<encoded-resource>" segment.
	return fmt.Sprintf("%s/%s/%s/", rwMetadataPrefix, kind, base64.RawURLEncoding.EncodeToString([]byte(resourceKey)))
}
