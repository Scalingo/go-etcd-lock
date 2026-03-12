package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gopkg.in/errgo.v1"
)

func TestRWLockAcquireRead(t *testing.T) {
	locker := testRWLocker()

	t.Run("read locks can be acquired concurrently", func(t *testing.T) {
		lock1, err := locker.AcquireRead("/rw-read-shared", 3)
		require.NoError(t, err)
		defer lock1.Release()

		lock2, err := locker.AcquireRead("/rw-read-shared", 3)
		require.NoError(t, err)
		defer lock2.Release()
	})

	t.Run("a read lock cannot be acquired while a writer holds the lock", func(t *testing.T) {
		lock, err := locker.AcquireWrite("/rw-read-blocked", 3)
		require.NoError(t, err)
		defer lock.Release()

		readLock, err := locker.AcquireRead("/rw-read-blocked", 3)
		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, readLock)
	})

	t.Run("a legacy write lock blocks a read lock", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := legacyLocker.Acquire("/rw-legacy-write-blocks-read", 3)
		require.NoError(t, err)
		defer lock.Release()

		readLock, err := locker.AcquireRead("/rw-legacy-write-blocks-read", 3)
		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, readLock)
	})
}

func TestRWLockAcquireWrite(t *testing.T) {
	locker := testRWLocker()

	t.Run("a write lock cannot be acquired while readers hold the lock", func(t *testing.T) {
		lock, err := locker.AcquireRead("/rw-write-blocked", 3)
		require.NoError(t, err)
		defer lock.Release()

		writeLock, err := locker.AcquireWrite("/rw-write-blocked", 3)
		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, writeLock)
	})

	t.Run("wait acquire write waits for readers to drain", func(t *testing.T) {
		lock, err := locker.AcquireRead("/rw-write-wait", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		t1 := time.Now()
		writeLock, err := locker.WaitAcquireWrite("/rw-write-wait", 2)
		t2 := time.Now()

		require.NoError(t, err)
		require.NotNil(t, writeLock)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
		require.NoError(t, writeLock.Release())
	})

	t.Run("a writer waits for all active readers", func(t *testing.T) {
		lock1, err := locker.AcquireRead("/rw-multi-reader-block", 3)
		require.NoError(t, err)
		defer lock1.Release()

		lock2, err := locker.AcquireRead("/rw-multi-reader-block", 3)
		require.NoError(t, err)
		defer lock2.Release()

		writeLock, err := locker.AcquireWrite("/rw-multi-reader-block", 3)
		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, writeLock)
	})
}

func TestRWLockMigration(t *testing.T) {
	rwLocker := testRWLocker()

	t.Run("a legacy write lock waits for active read locks", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := rwLocker.AcquireRead("/rw-read-blocks-legacy-write", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		t1 := time.Now()
		writeLock, err := legacyLocker.WaitAcquire("/rw-read-blocks-legacy-write", 2)
		t2 := time.Now()

		require.NoError(t, err)
		require.NotNil(t, writeLock)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
		require.NoError(t, writeLock.Release())
	})

	t.Run("an rw write lock waits for a legacy write lock", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := legacyLocker.Acquire("/legacy-write-blocks-rw-write", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		t1 := time.Now()
		writeLock, err := rwLocker.WaitAcquireWrite("/legacy-write-blocks-rw-write", 2)
		t2 := time.Now()

		require.NoError(t, err)
		require.NotNil(t, writeLock)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
		require.NoError(t, writeLock.Release())
	})

	t.Run("an rw read lock waits for a legacy write lock", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := legacyLocker.Acquire("/legacy-write-blocks-rw-read", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		t1 := time.Now()
		readLock, err := rwLocker.WaitAcquireRead("/legacy-write-blocks-rw-read", 2)
		t2 := time.Now()

		require.NoError(t, err)
		require.NotNil(t, readLock)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
		require.NoError(t, readLock.Release())
	})

	t.Run("wait acquire read waits behind a pending legacy writer", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		readLock, err := rwLocker.AcquireRead("/rw-pending-legacy-writer", 1)
		require.NoError(t, err)
		require.NotNil(t, readLock)

		writeErr := make(chan error, 1)
		writeReady := make(chan Lock, 1)
		writeAt := make(chan time.Time, 1)
		go func() {
			lock, err := legacyLocker.WaitAcquire("/rw-pending-legacy-writer", 1)
			writeErr <- err
			writeReady <- lock
			writeAt <- time.Now()
		}()

		waitUntilReadIsBlockedByWriter(t, rwLocker, "/rw-pending-legacy-writer")

		readErr := make(chan error, 1)
		readReady := make(chan Lock, 1)
		readAt := make(chan time.Time, 1)
		go func() {
			lock, err := rwLocker.WaitAcquireRead("/rw-pending-legacy-writer", 1)
			readErr <- err
			readReady <- lock
			readAt <- time.Now()
		}()

		require.NoError(t, <-writeErr)
		writeLock := <-writeReady
		require.NotNil(t, writeLock)
		writerAcquiredAt := <-writeAt

		select {
		case err := <-readErr:
			t.Fatalf("reader acquired before the legacy writer was released: %v", err)
		default:
		}

		require.NoError(t, writeLock.Release())

		require.NoError(t, <-readErr)
		nextReadLock := <-readReady
		require.NotNil(t, nextReadLock)
		readerAcquiredAt := <-readAt

		assert.True(t, readerAcquiredAt.After(writerAcquiredAt) || readerAcquiredAt.Equal(writerAcquiredAt))
		require.NoError(t, nextReadLock.Release())
	})

	t.Run("a pending legacy writer also blocks new readers after multiple active readers", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		readLock1, err := rwLocker.AcquireRead("/rw-many-readers-legacy-writer", 1)
		require.NoError(t, err)
		require.NotNil(t, readLock1)

		readLock2, err := rwLocker.AcquireRead("/rw-many-readers-legacy-writer", 1)
		require.NoError(t, err)
		require.NotNil(t, readLock2)

		writeErr := make(chan error, 1)
		writeReady := make(chan Lock, 1)
		go func() {
			lock, err := legacyLocker.WaitAcquire("/rw-many-readers-legacy-writer", 1)
			writeErr <- err
			writeReady <- lock
		}()

		waitUntilReadIsBlockedByWriter(t, rwLocker, "/rw-many-readers-legacy-writer")

		readErr := make(chan error, 1)
		readReady := make(chan Lock, 1)
		go func() {
			lock, err := rwLocker.WaitAcquireRead("/rw-many-readers-legacy-writer", 1)
			readReady <- lock
			readErr <- err
		}()

		require.NoError(t, <-writeErr)
		writeLock := <-writeReady
		require.NotNil(t, writeLock)

		select {
		case err := <-readErr:
			t.Fatalf("reader acquired before the pending legacy writer was released: %v", err)
		default:
		}

		require.NoError(t, writeLock.Release())
		require.NoError(t, <-readErr)
		nextReadLock := <-readReady
		require.NotNil(t, nextReadLock)
		require.NoError(t, nextReadLock.Release())
	})
}

func TestRWLockWait(t *testing.T) {
	locker := testRWLocker()

	t.Run("wait blocks until active readers are gone", func(t *testing.T) {
		lock, err := locker.AcquireRead("/rw-wait", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		t1 := time.Now()
		err = locker.Wait("/rw-wait")
		t2 := time.Now()

		require.NoError(t, err)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
	})

	t.Run("wait blocks until a legacy writer is gone", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := legacyLocker.Acquire("/legacy-writer-rw-wait", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		t1 := time.Now()
		err = locker.Wait("/legacy-writer-rw-wait")
		t2 := time.Now()

		require.NoError(t, err)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
	})
}

func TestRWLockTimeouts(t *testing.T) {
	t.Run("wait acquire read stops at max try lock timeout behind a legacy writer", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		rwLocker := testRWLocker(WithMaxTryLockTimeout(time.Second))

		lock, err := legacyLocker.Acquire("/legacy-write-read-timeout", 3)
		require.NoError(t, err)
		defer lock.Release()

		t1 := time.Now()
		readLock, err := rwLocker.WaitAcquireRead("/legacy-write-read-timeout", 3)
		t2 := time.Now()

		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, readLock)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
	})

	t.Run("legacy wait acquire stops at max try lock timeout behind an rw reader", func(t *testing.T) {
		legacyLocker := testLegacyLocker(WithMaxTryLockTimeout(time.Second))
		rwLocker := testRWLocker()

		lock, err := rwLocker.AcquireRead("/rw-read-legacy-timeout", 3)
		require.NoError(t, err)
		defer lock.Release()

		t1 := time.Now()
		writeLock, err := legacyLocker.WaitAcquire("/rw-read-legacy-timeout", 3)
		t2 := time.Now()

		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, writeLock)
		assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
	})
}

func TestRWLockRelease(t *testing.T) {
	rwLocker := testRWLocker()

	t.Run("releasing a read lock unblocks a legacy writer immediately", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := rwLocker.AcquireRead("/rw-release-read", 3)
		require.NoError(t, err)
		require.NotNil(t, lock)

		require.NoError(t, lock.Release())

		writeLock, err := legacyLocker.Acquire("/rw-release-read", 3)
		require.NoError(t, err)
		require.NotNil(t, writeLock)
		require.NoError(t, writeLock.Release())
	})

	t.Run("releasing a legacy writer unblocks a read lock immediately", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := legacyLocker.Acquire("/legacy-release-write", 3)
		require.NoError(t, err)
		require.NotNil(t, lock)

		require.NoError(t, lock.Release())

		readLock, err := rwLocker.AcquireRead("/legacy-release-write", 3)
		require.NoError(t, err)
		require.NotNil(t, readLock)
		require.NoError(t, readLock.Release())
	})

	t.Run("after expiration, release of a read lock should not produce an error", func(t *testing.T) {
		lock, err := rwLocker.AcquireRead("/rw-release-expired", 1)
		require.NoError(t, err)
		require.NotNil(t, lock)

		time.Sleep(2 * time.Second)

		err = lock.Release()
		require.NoError(t, err)
	})

	t.Run("release a nil rw lock should not panic", func(t *testing.T) {
		var lock *EtcdRWLock
		err := lock.Release()
		require.Error(t, err)
	})
}

func testRWLocker(opts ...EtcdLockerOpt) RWLocker {
	base := []EtcdLockerOpt{
		WithTryLockTimeout(500 * time.Millisecond),
		WithCooldownTryLockDuration(50 * time.Millisecond),
		WithMaxTryLockTimeout(5 * time.Second),
	}
	base = append(base, opts...)
	return NewEtcdRWLocker(client(), base...)
}

func testLegacyLocker(opts ...EtcdLockerOpt) Locker {
	base := []EtcdLockerOpt{
		WithTryLockTimeout(500 * time.Millisecond),
		WithCooldownTryLockDuration(50 * time.Millisecond),
		WithMaxTryLockTimeout(5 * time.Second),
	}
	base = append(base, opts...)
	return NewEtcdLocker(client(), base...)
}

func waitUntilReadIsBlockedByWriter(t *testing.T, locker RWLocker, key string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lock, err := locker.AcquireRead(key, 1)
		if err == nil {
			require.NoError(t, lock.Release())
			time.Sleep(20 * time.Millisecond)
			continue
		}

		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		return
	}

	t.Fatalf("reader was never blocked by a pending writer for key %q", key)
}
