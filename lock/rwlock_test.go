package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gopkg.in/errgo.v1"
)

func TestRWLockAcquireRead(t *testing.T) {
	locker := NewEtcdRWLocker(
		client(),
		WithTryLockTimeout(500*time.Millisecond),
		WithCooldownTryLockDuration(50*time.Millisecond),
	)

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
		legacyLocker := NewEtcdLocker(
			client(),
			WithTryLockTimeout(500*time.Millisecond),
			WithCooldownTryLockDuration(50*time.Millisecond),
		)
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
	locker := NewEtcdRWLocker(
		client(),
		WithTryLockTimeout(500*time.Millisecond),
		WithCooldownTryLockDuration(50*time.Millisecond),
		WithMaxTryLockTimeout(5*time.Second),
	)

	t.Run("a write lock cannot be acquired while readers hold the lock", func(t *testing.T) {
		lock, err := locker.AcquireRead("/rw-write-blocked", 3)
		require.NoError(t, err)
		defer lock.Release()

		writeLock, err := locker.AcquireWrite("/rw-write-blocked", 3)
		require.Error(t, err)
		assert.IsType(t, &ErrAlreadyLocked{}, errgo.Cause(err))
		assert.Nil(t, writeLock)
	})

	t.Run("a legacy write lock waits for active read locks", func(t *testing.T) {
		legacyLocker := NewEtcdLocker(
			client(),
			WithTryLockTimeout(500*time.Millisecond),
			WithCooldownTryLockDuration(50*time.Millisecond),
			WithMaxTryLockTimeout(5*time.Second),
		)

		lock, err := locker.AcquireRead("/rw-read-blocks-legacy-write", 1)
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

	t.Run("wait acquire read waits behind a pending legacy writer", func(t *testing.T) {
		legacyLocker := NewEtcdLocker(
			client(),
			WithTryLockTimeout(500*time.Millisecond),
			WithCooldownTryLockDuration(50*time.Millisecond),
			WithMaxTryLockTimeout(5*time.Second),
		)

		readLock, err := locker.AcquireRead("/rw-pending-legacy-writer", 1)
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

		time.Sleep(200 * time.Millisecond)

		readErr := make(chan error, 1)
		readReady := make(chan Lock, 1)
		readAt := make(chan time.Time, 1)
		t1 := time.Now()
		go func() {
			lock, err := locker.WaitAcquireRead("/rw-pending-legacy-writer", 1)
			readErr <- err
			readReady <- lock
			readAt <- time.Now()
		}()

		require.NoError(t, <-writeErr)
		writeLock := <-writeReady
		require.NotNil(t, writeLock)
		writerAcquiredAt := <-writeAt

		time.Sleep(200 * time.Millisecond)
		require.NoError(t, writeLock.Release())

		require.NoError(t, <-readErr)
		nextReadLock := <-readReady
		require.NotNil(t, nextReadLock)
		readerAcquiredAt := <-readAt

		assert.GreaterOrEqual(t, int(readerAcquiredAt.Sub(t1).Seconds()), 1)
		assert.False(t, readerAcquiredAt.Before(writerAcquiredAt))
		require.NoError(t, nextReadLock.Release())
	})
}

func TestRWLockWait(t *testing.T) {
	locker := NewEtcdRWLocker(
		client(),
		WithTryLockTimeout(500*time.Millisecond),
		WithCooldownTryLockDuration(50*time.Millisecond),
	)

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
}
