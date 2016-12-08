package lock

import (
	"sync"
	"time"
)

type MockLocker struct {
	*sync.Mutex
	locks map[string]*MockLock
}

type MockLock struct {
	mutex  *sync.Mutex
	locked bool
}

func NewMockLocker() *MockLocker {
	return &MockLocker{
		Mutex: &sync.Mutex{},
		locks: make(map[string]*MockLock),
	}
}

func (locker *MockLocker) Acquire(path string, ttl uint64) (Lock, error) {
	locker.Lock()
	defer locker.Unlock()
	m, ok := locker.locks[path]
	if !ok {
		locker.locks[path] = &MockLock{mutex: &sync.Mutex{}}
		m = locker.locks[path]
	}

	if m.locked {
		return nil, &Error{}
	} else {
		m.mutex.Lock()
		m.locked = true
	}

	if ttl > 0 {
		go func() {
			time.Sleep(time.Duration(ttl) * time.Second)
			m.Release()
		}()
	}

	return m, nil
}

func (locker *MockLocker) WaitAcquire(path string, ttl uint64) (Lock, error) {
	var lock Lock
	var err error = &Error{}
	for err != nil {
		lock, err = locker.Acquire(path, ttl)
		if err != nil {
			break
		}
	}
	return lock, nil
}

func (locker *MockLocker) Wait(path string) error {
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

func (lock *MockLock) Release() error {
	lock.locked = false
	lock.mutex.Unlock()
	return nil
}
