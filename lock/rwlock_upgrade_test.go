package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	etcdv3 "go.etcd.io/etcd/client/v3"
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

	t.Run("wait upgrade retries writer intent publication", func(t *testing.T) {
		locker := mustRWLocker(t, testRWLocker(
			WithTryLockTimeout(100*time.Millisecond),
			WithCooldownTryLockDuration(50*time.Millisecond),
			WithMaxTryLockTimeout(2*time.Second),
		))
		liveClient := locker.etcdClient()
		readLock, err := locker.AcquireRead("/rw-upgrade-intent-retry", 3)
		require.NoError(t, err)
		rwReadLock := mustRWReadLock(t, readLock)

		brokenClient := newBrokenEtcdClient(t)
		locker.setEtcdClient(brokenClient)
		t.Cleanup(func() {
			locker.setEtcdClient(liveClient)
			require.NoError(t, brokenClient.Close())
		})

		writeErr := make(chan error, 1)
		writeReady := make(chan Lock, 1)
		t1 := time.Now()
		go func() {
			lock, err := rwReadLock.WaitUpgrade()
			writeErr <- err
			writeReady <- lock
		}()

		waitUntilUpgradeInProgress(t, rwReadLock)
		time.Sleep(200 * time.Millisecond)
		locker.setEtcdClient(liveClient)

		require.NoError(t, <-writeErr)
		writeLock := <-writeReady
		require.NotNil(t, writeLock)
		duration := time.Since(t1)
		require.GreaterOrEqual(t, duration, 200*time.Millisecond)
		require.Less(t, duration, 2*time.Second)
		require.NoError(t, writeLock.Release())
	})

	t.Run("release during a pending upgrade cancels it and frees the read lock", func(t *testing.T) {
		locker := mustRWLocker(t, testRWLocker(
			WithTryLockTimeout(100*time.Millisecond),
			WithCooldownTryLockDuration(50*time.Millisecond),
			WithMaxTryLockTimeout(2*time.Second),
		))
		liveClient := locker.etcdClient()
		readLock, err := locker.AcquireRead("/rw-upgrade-release-cancels", 3)
		require.NoError(t, err)
		rwReadLock := mustRWReadLock(t, readLock)

		brokenClient := newBrokenEtcdClient(t)
		locker.setEtcdClient(brokenClient)
		t.Cleanup(func() {
			locker.setEtcdClient(liveClient)
			require.NoError(t, brokenClient.Close())
		})

		writeErr := make(chan error, 1)
		go func() {
			_, err := rwReadLock.WaitUpgrade()
			writeErr <- err
		}()

		waitUntilUpgradeInProgress(t, rwReadLock)
		time.Sleep(150 * time.Millisecond)
		require.NoError(t, readLock.Release())

		err = <-writeErr
		require.Error(t, err)
		require.ErrorContains(t, err, "lock released during upgrade")

		locker.setEtcdClient(liveClient)
		writeLock, err := locker.AcquireWrite("/rw-upgrade-release-cancels", 3)
		require.NoError(t, err)
		require.NotNil(t, writeLock)
		require.NoError(t, writeLock.Release())
	})
}

func mustRWLocker(t *testing.T, locker RWLocker) *EtcdRWLocker {
	t.Helper()

	rwLocker, ok := locker.(*EtcdRWLocker)
	require.True(t, ok)
	return rwLocker
}

func newBrokenEtcdClient(t *testing.T) *etcdv3.Client {
	t.Helper()

	client, err := etcdv3.New(etcdv3.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		DialTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	return client
}

func waitUntilUpgradeInProgress(t *testing.T, lock *EtcdRWLock) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		lock.Lock()
		upgrading := lock.upgrading
		lock.Unlock()
		if upgrading {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("upgrade never entered progress state")
}
