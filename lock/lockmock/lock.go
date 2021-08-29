package lockmock

import (
	"sync"
	"time"

	lock "github.com/skvoch/go-etcd-lock/v5/lock"
)

type MockLockerMutex struct {
	*sync.Mutex
	locks map[string]*MockLockMutex
}

type MockLockMutex struct {
	mutex  *sync.Mutex
	locked bool
}

func New() *MockLockerMutex {
	return &MockLockerMutex{
		Mutex: &sync.Mutex{},
		locks: make(map[string]*MockLockMutex),
	}
}

func (locker *MockLockerMutex) Acquire(path string, ttl uint64) (lock.Lock, error) {
	locker.Lock()
	defer locker.Unlock()
	m, ok := locker.locks[path]
	if !ok {
		locker.locks[path] = &MockLockMutex{mutex: &sync.Mutex{}}
		m = locker.locks[path]
	}

	if m.locked {
		return nil, &lock.ErrAlreadyLocked{}
	}

	m.mutex.Lock()
	m.locked = true

	if ttl > 0 {
		go func() {
			time.Sleep(time.Duration(ttl) * time.Second)
			m.Release()
		}()
	}

	return m, nil
}

func (locker *MockLockerMutex) WaitAcquire(path string, ttl uint64) (lock.Lock, error) {
	var l lock.Lock
	var err error = &lock.ErrAlreadyLocked{}
	for err != nil {
		l, err = locker.Acquire(path, ttl)
		if err != nil {
			break
		}
	}
	return l, nil
}

func (locker *MockLockerMutex) Wait(path string) error {
	locker.Lock()
	defer locker.Unlock()

	l, ok := locker.locks[path]
	if !ok {
		return nil
	}

	if l.locked {
		l.mutex.Lock()
		l.mutex.Unlock()
	}

	return nil
}

func (lock *MockLockMutex) Release() error {
	lock.locked = false
	lock.mutex.Unlock()
	return nil
}
