package lock

import (
	"context"

	"gopkg.in/errgo.v1"
)

func (l *EtcdLock) Release() error {
	if l == nil {
		return errgo.New("nil lock")
	}
	l.Lock()
	defer l.Unlock()

	unlockErr := l.mutex.Unlock(context.Background())
	if l.intentKey != "" {
		_, err := l.client.Delete(context.Background(), l.intentKey)
		if unlockErr == nil && err != nil {
			unlockErr = err
		}
	}
	_ = l.session.Close()
	return unlockErr
}
