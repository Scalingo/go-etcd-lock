package lock

import (
	"context"
	"time"

	"github.com/Scalingo/go-utils/errors/v3"
)

type releaseOrigin uint8

const (
	manualRelease releaseOrigin = iota
	ttlRelease
)

func scheduleRelease(ctx context.Context, lock Lock, ttl int) {
	ctx = context.WithoutCancel(ctx)
	time.AfterFunc(time.Duration(ttl)*time.Second, func() {
		switch l := lock.(type) {
		case *EtcdLock:
			_ = l.release(ctx, ttlRelease)
		case *EtcdRWLock:
			_ = l.release(ctx, ttlRelease)
		default:
			_ = lock.Release()
		}
	})
}

func (l *EtcdLock) Release() error {
	return l.release(context.Background(), manualRelease)
}

func (l *EtcdLock) release(ctx context.Context, origin releaseOrigin) (resultErr error) {
	if l == nil {
		return errors.New(ctx, "nil lock")
	}
	l.Lock()
	defer l.Unlock()
	if origin == ttlRelease && l.released {
		return nil
	}
	released := false
	defer func() {
		result := okResult
		if resultErr != nil {
			result = errorResult
		}
		l.metrics.recordRelease(ctx, writeLock, result, released)
	}()

	unlockErr := l.mutex.Unlock(ctx)
	if unlockErr != nil {
		unlockErr = errors.Wrap(ctx, unlockErr, "unlock lock")
	} else if !l.released {
		l.released = true
		released = true
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
