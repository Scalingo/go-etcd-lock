package lockmock

import (
	"sync"
	"time"

	lock "github.com/Scalingo/go-etcd-lock/lock"
)

type LockerMock struct {
	*sync.Mutex
	locks map[string]*MockLock
}

type MockLock struct {
	mutex  *sync.Mutex
	locked bool
}

func New() *LockerMock {
	return &LockerMock{
		Mutex: &sync.Mutex{},
		locks: make(map[string]*MockLock),
	}
}

func (locker *LockerMock) Acquire(path string, ttl uint64) (lock.Lock, error) {
	locker.Lock()
	defer locker.Unlock()
	m, ok := locker.locks[path]
	if !ok {
		locker.locks[path] = &MockLock{mutex: &sync.Mutex{}}
		m = locker.locks[path]
	}

	if m.locked {
		return nil, &lock.Error{}
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

func (locker *LockerMock) WaitAcquire(path string, ttl uint64) (lock.Lock, error) {
	var l lock.Lock
	var err error = &lock.Error{}
	for err != nil {
		l, err = locker.Acquire(path, ttl)
		if err != nil {
			break
		}
	}
	return l, nil
}

func (locker *LockerMock) Wait(path string) error {
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
