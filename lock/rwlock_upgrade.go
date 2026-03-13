package lock

import (
	"context"
	"sync"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"gopkg.in/errgo.v1"
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
	if l == nil {
		return nil, errgo.New("nil lock")
	}
	deadline := time.Now().Add(l.locker.maxTryLockTimeout)

	// Upgrade is deliberately non-atomic: we first mark the intent to write,
	// then release the read entry, then compete for the legacy write lock.
	l.Lock()
	if l.released {
		l.Unlock()
		return nil, errgo.New("lock already released")
	}
	if l.upgrading {
		l.Unlock()
		return nil, errgo.New("lock upgrade already in progress")
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
			_ = closeRWSession(writeSession)
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
		closeErr := closeRWSession(writeSession)
		return nil, noteAcquireFailure(err, closeErr, "fail to upgrade read lock")
	}

	if l.isReleaseRequested() {
		return nil, l.abortUpgrade(readKey, readSession, writeSession)
	}

	releaseErr := l.releaseForUpgrade(readKey, readSession)
	if releaseErr != nil {
		l.finishUpgrade()
		closeErr := closeRWSession(writeSession)
		return nil, noteAcquireFailure(releaseErr, closeErr, "fail to upgrade read lock")
	}

	writeLock, err := locker.acquireWriteWithSession(resourceKey, writeSession, ttl, wait, true, deadline, nil)
	if err != nil {
		return nil, errgo.Notef(err, "fail to upgrade read lock")
	}
	// Ownership of the session moves to the returned write lock.
	writeSession = nil

	return writeLock, nil
}

func (l *EtcdRWLock) releaseForUpgrade(readKey string, session *concurrency.Session) error {
	l.Lock()
	defer l.Unlock()

	_, err := l.client.Delete(context.Background(), readKey)
	closeErr := closeRWSession(session)
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

func (locker *EtcdRWLocker) acquireWriteWithSession(resourceKey string, session *concurrency.Session, ttl int, wait bool, intentCreated bool, deadline time.Time, abortRequested func() bool) (Lock, error) {
	// Upgrade reuses the same retry model as the normal write path:
	// per-attempt tryLockTimeout, cooldown between retries, and a global
	// maxTryLockTimeout for the whole wait.
	mutex := concurrency.NewMutex(session, resourceKey)
	tryLockErr := error(nil)
	writeLocker := locker.writeLocker()
	for {
		if abortRequested != nil && abortRequested() {
			closeErr := closeRWSession(session)
			return nil, noteAcquireFailure(errUpgradeReleased, closeErr, "upgrade cancelled by release")
		}
		if time.Now().After(deadline) {
			closeErr := closeRWSession(session)
			if tryLockErr == context.DeadlineExceeded {
				return nil, &ErrAlreadyLocked{}
			}
			return nil, noteAcquireFailure(errgo.Notef(tryLockErr, "fail to acquire lock"), closeErr, "fail to acquire write lock")
		}

		if !intentCreated {
			tryLockErr = writeLocker.createWriterIntent(rwWriterIntentKey(resourceKey, session.Lease()), session.Lease())
			if tryLockErr == nil {
				intentCreated = true
			} else {
				time.Sleep(locker.cooldownTryLockDuration)
				continue
			}
		}

		tryLockErr = writeLocker.tryLock(mutex)
		shouldWait := wait && tryLockErr == context.DeadlineExceeded
		shouldRetry := shouldWait || (tryLockErr != nil && tryLockErr != context.DeadlineExceeded)
		if shouldRetry {
			time.Sleep(locker.cooldownTryLockDuration)
			continue
		}
		if tryLockErr == context.DeadlineExceeded {
			closeErr := closeRWSession(session)
			if closeErr != nil {
				return nil, noteAcquireFailure(&ErrAlreadyLocked{}, closeErr, "fail to acquire write lock")
			}
			return nil, &ErrAlreadyLocked{}
		}
		break
	}

	lock := &EtcdLock{mutex: mutex, Mutex: &sync.Mutex{}, session: session}
	time.AfterFunc(time.Duration(ttl)*time.Second, func() {
		_ = lock.Release()
	})

	return lock, nil
}

var errUpgradeReleased = errgo.New("lock released during upgrade")

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

		lastErr = locker.writeLocker().createWriterIntent(intentKey, leaseID)
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
	l.Lock()
	l.released = true
	l.upgrading = false
	l.releaseRequested = false
	l.lockKey = ""
	l.session = nil
	l.Unlock()

	_, err := l.client.Delete(context.Background(), readKey)
	readCloseErr := closeRWSession(readSession)
	writeCloseErr := closeRWSession(writeSession)
	cleanupErr := firstNonNil(readCloseErr, writeCloseErr)
	if err != nil {
		return noteAcquireFailure(errUpgradeReleased, err, "upgrade cancelled by release")
	}
	if cleanupErr != nil {
		return noteAcquireFailure(errUpgradeReleased, cleanupErr, "upgrade cancelled by release")
	}

	return errUpgradeReleased
}

func firstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
