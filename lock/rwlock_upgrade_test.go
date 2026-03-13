package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRWLockUpgrade(t *testing.T) {
	locker := testRWLocker()

	t.Run("a read lock can be upgraded to a write lock", func(t *testing.T) {
		readLock, err := locker.AcquireRead("/rw-upgrade", 3)
		require.NoError(t, err)

		writeLock, err := mustRWReadLock(t, readLock).Upgrade()
		require.NoError(t, err)
		require.NotNil(t, writeLock)

		require.NoError(t, readLock.Release())
		require.NoError(t, writeLock.Release())
	})

	t.Run("wait upgrade waits for another reader to drain", func(t *testing.T) {
		readLock1, err := locker.AcquireRead("/rw-upgrade-wait", 3)
		require.NoError(t, err)
		rwReadLock1 := mustRWReadLock(t, readLock1)

		readLock2, err := locker.AcquireRead("/rw-upgrade-wait", 1)
		require.NoError(t, err)
		require.NotNil(t, readLock2)

		t1 := time.Now()
		writeLock, err := rwReadLock1.WaitUpgrade()
		t2 := time.Now()

		require.NoError(t, err)
		require.NotNil(t, writeLock)
		assertWaitAround(t, t2.Sub(t1))
		require.NoError(t, writeLock.Release())
	})

	t.Run("a pending upgrade blocks new readers", func(t *testing.T) {
		readLock1, err := locker.AcquireRead("/rw-upgrade-blocks-readers", 3)
		require.NoError(t, err)
		rwReadLock1 := mustRWReadLock(t, readLock1)

		readLock2, err := locker.AcquireRead("/rw-upgrade-blocks-readers", 1)
		require.NoError(t, err)
		require.NotNil(t, readLock2)

		writeErr := make(chan error, 1)
		writeReady := make(chan Lock, 1)
		go func() {
			lock, err := rwReadLock1.WaitUpgrade()
			writeErr <- err
			writeReady <- lock
		}()

		waitUntilWriterIntentExists(t, "/rw-upgrade-blocks-readers")

		readErr := make(chan error, 1)
		readReady := make(chan Lock, 1)
		go func() {
			lock, err := locker.WaitAcquireRead("/rw-upgrade-blocks-readers", 1)
			readErr <- err
			readReady <- lock
		}()

		require.NoError(t, <-writeErr)
		writeLock := <-writeReady
		require.NotNil(t, writeLock)

		select {
		case err := <-readErr:
			t.Fatalf("reader acquired before the upgraded writer was released: %v", err)
		default:
		}

		require.NoError(t, writeLock.Release())
		require.NoError(t, <-readErr)
		nextReadLock := <-readReady
		require.NotNil(t, nextReadLock)
		require.NoError(t, nextReadLock.Release())
	})
}
