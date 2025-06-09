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

	defer l.session.Close()
	return l.mutex.Unlock(context.Background())
}

func (l *EtcdLock) ReleaseOnTimeout() error {
	if l == nil {
		return errgo.New("nil lock")
	}
	l.Lock()
	defer l.Unlock()

	defer l.session.Close()

	if l.callbackBeforeRelease != nil {
		l.callbackBeforeRelease()
	}
	return l.mutex.Unlock(context.Background())
}
