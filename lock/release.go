package lock

import (
	"context"
	"time"

	"github.com/Scalingo/go-utils/errors/v3"
)

func scheduleRelease(lock Lock, ttl int) {
	time.AfterFunc(time.Duration(ttl)*time.Second, func() {
		_ = lock.Release()
	})
}

func (l *EtcdLock) Release() error {
	ctx := context.Background()
	if l == nil {
		return errors.New(ctx, "nil lock")
	}
	l.Lock()
	defer l.Unlock()

	unlockErr := l.mutex.Unlock(ctx)
	if unlockErr != nil {
		unlockErr = errors.Wrap(ctx, unlockErr, "unlock lock")
	}

	var intentErr error
	if l.intentKey != "" {
		_, err := l.client.Delete(ctx, l.intentKey)
		if err != nil {
			intentErr = errors.Wrap(ctx, err, "delete writer intent")
		}
	}

	var closeErr error
	err := l.session.Close()
	if err != nil {
		closeErr = errors.Wrap(ctx, err, "close lock session")
	}

	return errors.Join(unlockErr, intentErr, closeErr)
}
