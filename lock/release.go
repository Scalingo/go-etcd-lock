package lock

import "gopkg.in/errgo.v1"

func (l *EtcdLock) Release() error {
	if l == nil {
		return errgo.New("nil lock")
	}

	l.mutex.Unlock()
	return nil
}
