package lock

import (
	"context"
	"sync"
	"time"

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

	writeLocker := locker.writeLocker()
	intentKey := rwWriterIntentKey(resourceKey, writeSession.Lease())
	if err := writeLocker.createWriterIntent(intentKey, writeSession.Lease()); err != nil {
		l.finishUpgrade()
		closeErr := closeRWSession(writeSession)
		return nil, noteAcquireFailure(err, closeErr, "fail to upgrade read lock")
	}

	releaseErr := l.releaseForUpgrade(readKey, readSession)
	if releaseErr != nil {
		l.finishUpgrade()
		closeErr := closeRWSession(writeSession)
		return nil, noteAcquireFailure(releaseErr, closeErr, "fail to upgrade read lock")
	}

	writeLock, err := locker.acquireWriteWithSession(resourceKey, writeSession, ttl, wait, true)
	if err != nil {
		return nil, errgo.Notef(err, "fail to upgrade read lock")
	}

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
}

func (locker *EtcdRWLocker) acquireWriteWithSession(resourceKey string, session *concurrency.Session, ttl int, wait bool, intentCreated bool) (Lock, error) {
	mutex := concurrency.NewMutex(session, resourceKey)
	timeout := time.NewTimer(locker.maxTryLockTimeout)
	defer timeout.Stop()

	tryLockErr := error(nil)
	writeLocker := locker.writeLocker()
	for {
		select {
		case <-timeout.C:
			closeErr := closeRWSession(session)
			if tryLockErr == context.DeadlineExceeded {
				return nil, &ErrAlreadyLocked{}
			}
			return nil, noteAcquireFailure(errgo.Notef(tryLockErr, "fail to acquire lock"), closeErr, "fail to acquire write lock")
		default:
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
