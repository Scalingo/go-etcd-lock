package lock

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/Scalingo/go-utils/errors/v3"
)

// Upgrade tries to turn a read lock into a write lock without waiting.
// The read lock is released during the upgrade attempt, so callers must
// re-check the protected resource once the write lock is acquired.
func (l *EtcdRWLock) Upgrade() (Lock, error) {
	return l.upgrade(false)
}

// WaitUpgrade tries to turn a read lock into a write lock, waiting up to the
// configured max timeout for competing readers or writers to drain.
// The read lock is released during the upgrade attempt, so callers must
// re-check the protected resource once the write lock is acquired.
func (l *EtcdRWLock) WaitUpgrade() (Lock, error) {
	return l.upgrade(true)
}

func (l *EtcdRWLock) upgrade(wait bool) (Lock, error) {
	ctx := context.Background()

	if l == nil {
		return nil, errors.New(ctx, "nil lock")
	}
	deadline := time.Now().Add(l.locker.maxTryLockTimeout)

	// Upgrade is deliberately non-atomic: we first mark the intent to write,
	// then release the read entry, then compete for the legacy write lock.
	l.Lock()
	if l.released {
		l.Unlock()
		return nil, errors.New(ctx, "lock already released")
	}
	if l.upgrading {
		l.Unlock()
		return nil, errors.New(ctx, "lock upgrade already in progress")
	}
	l.upgrading = true
	locker := l.locker
	resourceKey := l.resourceKey
	ttl := l.ttl
	readKey := l.lockKey
	readSession := l.session
	client := l.client
	l.Unlock()

	writeSession, err := concurrency.NewSession(client, concurrency.WithTTL(ttl))
	if err != nil {
		l.finishUpgrade()
		return nil, err
	}
	defer func() {
		if writeSession != nil {
			_ = closeRWSessionWithCtx(ctx, writeSession)
		}
	}()

	intentKey := rwWriterIntentKey(resourceKey, writeSession.Lease())
	// Publishing writer intent before dropping the read lock prevents later
	// readers from slipping in during the upgrade window.
	err = locker.createWriterIntentWithRetry(intentKey, writeSession.Lease(), deadline, wait, l.isReleaseRequested)
	if err != nil {
		l.finishUpgrade()
		if err == errUpgradeReleased {
			return nil, l.abortUpgrade(readKey, readSession, writeSession)
		}
		closeErr := closeRWSessionWithCtx(ctx, writeSession)
		if closeErr != nil {
			return nil, errors.Wrapf(ctx, err, "upgrade read lock (cleanup: %v)", closeErr)
		}
		return nil, errors.Wrap(ctx, err, "upgrade read lock")
	}

	if l.isReleaseRequested() {
		return nil, l.abortUpgrade(readKey, readSession, writeSession)
	}

	releaseErr := l.releaseForUpgrade(ctx, readKey, readSession)
	if releaseErr != nil {
		l.finishUpgrade()
		closeErr := closeRWSessionWithCtx(ctx, writeSession)
		if closeErr != nil {
			return nil, errors.Wrapf(ctx, releaseErr, "upgrade read lock (cleanup: %v)", closeErr)
		}
		return nil, errors.Wrap(ctx, releaseErr, "upgrade read lock")
	}

	writeLock, err := locker.acquireWriteWithSession(ctx, resourceKey, writeSession, ttl, wait, true, deadline, nil)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "upgrade read lock")
	}
	// Ownership of the session moves to the returned write lock.
	writeSession = nil

	return writeLock, nil
}

func (l *EtcdRWLock) releaseForUpgrade(ctx context.Context, readKey string, session *concurrency.Session) error {
	l.Lock()
	defer l.Unlock()

	_, err := l.client.Delete(ctx, readKey)
	closeErr := closeRWSessionWithCtx(ctx, session)
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	l.released = true
	l.upgrading = false
	l.lockKey = ""
	l.session = nil

	return nil
}

func (l *EtcdRWLock) finishUpgrade() {
	l.Lock()
	defer l.Unlock()

	l.upgrading = false
	l.releaseRequested = false
}

func (locker *EtcdRWLocker) acquireWriteWithSession(ctx context.Context, resourceKey string, session *concurrency.Session, ttl int, wait bool, intentCreated bool, deadline time.Time, abortRequested func() bool) (Lock, error) {
	// Upgrade reuses the same retry model as the normal write path:
	// per-attempt tryLockTimeout, cooldown between retries, and a global
	// maxTryLockTimeout for the whole wait.
	mutex := concurrency.NewMutex(session, resourceKey)
	tryLockErr := error(nil)
	writeLocker := locker.writeLocker()
	intentKey := rwWriterIntentKey(resourceKey, session.Lease())
	for {
		if abortRequested != nil && abortRequested() {
			closeErr := closeRWSessionWithCtx(ctx, session)
			if closeErr != nil {
				return nil, errors.Wrapf(ctx, errUpgradeReleased, "upgrade cancelled by release (cleanup: %v)", closeErr)
			}
			return nil, errors.Wrap(ctx, errUpgradeReleased, "upgrade cancelled by release")
		}
		if time.Now().After(deadline) {
			closeErr := closeRWSessionWithCtx(ctx, session)
			if tryLockErr == context.DeadlineExceeded {
				return nil, &ErrAlreadyLocked{}
			}
			acquireErr := errors.Wrap(ctx, tryLockErr, "acquire lock")
			if closeErr != nil {
				return nil, errors.Wrapf(ctx, acquireErr, "acquire write lock (cleanup: %v)", closeErr)
			}
			return nil, errors.Wrap(ctx, acquireErr, "acquire write lock")
		}

		if !intentCreated {
			tryLockErr = writeLocker.createWriterIntent(ctx, intentKey, session.Lease())
			if tryLockErr == nil {
				intentCreated = true
			} else {
				time.Sleep(locker.cooldownTryLockDuration)
				continue
			}
		}

		tryLockErr = writeLocker.tryLock(ctx, mutex)
		shouldWait := wait && tryLockErr == context.DeadlineExceeded
		shouldRetry := shouldWait || (tryLockErr != nil && tryLockErr != context.DeadlineExceeded)
		if shouldRetry {
			time.Sleep(locker.cooldownTryLockDuration)
			continue
		}
		if tryLockErr == context.DeadlineExceeded {
			closeErr := closeRWSessionWithCtx(ctx, session)
			if closeErr != nil {
				return nil, errors.Wrapf(ctx, &ErrAlreadyLocked{}, "acquire write lock (cleanup: %v)", closeErr)
			}
			return nil, &ErrAlreadyLocked{}
		}
		break
	}

	lock := &EtcdLock{
		Mutex:   &sync.Mutex{},
		client:  writeLocker.client,
		mutex:   mutex,
		session: session,
	}
	if intentCreated {
		lock.intentKey = intentKey
	}
	err := writeLocker.waitForReaders(ctx, resourceKey, wait, deadline)
	if err != nil {
		releaseErr := releaseLockWithCtx(ctx, lock)
		var alreadyLocked *ErrAlreadyLocked
		if errors.As(err, &alreadyLocked) {
			if releaseErr != nil {
				return nil, errors.Wrapf(ctx, &ErrAlreadyLocked{}, "acquire write lock (cleanup: %v)", releaseErr)
			}
			return nil, errors.Wrap(ctx, err, "acquire write lock")
		}
		if releaseErr != nil {
			return nil, errors.Wrapf(ctx, err, "acquire write lock (cleanup: %v)", releaseErr)
		}
		return nil, errors.Wrap(ctx, err, "acquire write lock")
	}
	scheduleRelease(lock, ttl)

	return lock, nil
}

var errUpgradeReleased = errors.New(context.Background(), "lock released during upgrade")

func (locker *EtcdRWLocker) createWriterIntentWithRetry(intentKey string, leaseID etcdv3.LeaseID, deadline time.Time, wait bool, abortRequested func() bool) error {
	lastErr := error(nil)
	for {
		if abortRequested != nil && abortRequested() {
			return errUpgradeReleased
		}
		if time.Now().After(deadline) {
			if lastErr == nil {
				return context.DeadlineExceeded
			}
			return lastErr
		}

		lastErr = locker.writeLocker().createWriterIntent(context.Background(), intentKey, leaseID)
		if lastErr == nil {
			return nil
		}
		if !wait {
			return lastErr
		}
		time.Sleep(locker.cooldownTryLockDuration)
	}
}

func (l *EtcdRWLock) isReleaseRequested() bool {
	l.Lock()
	defer l.Unlock()

	return l.releaseRequested
}

func (l *EtcdRWLock) abortUpgrade(readKey string, readSession *concurrency.Session, writeSession *concurrency.Session) error {
	ctx := context.Background()

	l.Lock()
	l.released = true
	l.upgrading = false
	l.releaseRequested = false
	l.lockKey = ""
	l.session = nil
	l.Unlock()

	_, err := l.client.Delete(ctx, readKey)
	readCloseErr := closeRWSessionWithCtx(ctx, readSession)
	writeCloseErr := closeRWSessionWithCtx(ctx, writeSession)
	cleanupErr := firstNonNil(readCloseErr, writeCloseErr)
	if err != nil {
		return errors.Wrap(ctx, errUpgradeReleased, err.Error())
	}
	if cleanupErr != nil {
		return errors.Wrapf(ctx, errUpgradeReleased, "upgrade cancelled by release (cleanup: %v)", cleanupErr)
	}

	return errUpgradeReleased
}

func closeRWSessionWithCtx(ctx context.Context, session *concurrency.Session) error {
	if session == nil {
		return nil
	}

	err := session.Close()
	if err == nil || err == rpctypes.ErrLeaseNotFound {
		return nil
	}

	return errors.Wrap(ctx, err, "close rw lock session")
}

func releaseLockWithCtx(ctx context.Context, lock Lock) error {
	err := lock.Release()
	if err == nil {
		return nil
	}

	return errors.Wrap(ctx, err, "release lock")
}

func firstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
