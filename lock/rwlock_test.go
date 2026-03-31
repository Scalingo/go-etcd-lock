package lock

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Scalingo/go-utils/errors/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

func TestRWLockAcquireRead(t *testing.T) {
	locker := testRWLocker()

	t.Run("read locks can be acquired concurrently", func(t *testing.T) {
		lock1, err := locker.AcquireRead("/rw-read-shared", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock1.Release())
		})

		lock2, err := locker.AcquireRead("/rw-read-shared", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock2.Release())
		})
	})

	t.Run("a read lock cannot be acquired while a writer holds the lock", func(t *testing.T) {
		lock, err := locker.AcquireWrite("/rw-read-blocked", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock.Release())
		})

		readLock, err := locker.AcquireRead("/rw-read-blocked", 3)
		require.Error(t, err)
		assertAlreadyLocked(t, err)
		assert.Nil(t, readLock)
	})

	t.Run("a legacy write lock blocks a read lock", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		lock, err := legacyLocker.Acquire("/rw-legacy-write-blocks-read", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock.Release())
		})

		readLock, err := locker.AcquireRead("/rw-legacy-write-blocks-read", 3)
		require.Error(t, err)
		assertAlreadyLocked(t, err)
		assert.Nil(t, readLock)
	})
}

func TestRWLockAcquireWrite(t *testing.T) {
	locker := testRWLocker()

	t.Run("a write lock cannot be acquired while readers hold the lock", func(t *testing.T) {
		lock, err := locker.AcquireRead("/rw-write-blocked", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock.Release())
		})

		writeLock, err := locker.AcquireWrite("/rw-write-blocked", 3)
		require.Error(t, err)
		assertAlreadyLocked(t, err)
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
		assertWaitAround(t, t2.Sub(t1))
		require.NoError(t, writeLock.Release())
	})

	t.Run("a writer waits for all active readers", func(t *testing.T) {
		lock1, err := locker.AcquireRead("/rw-multi-reader-block", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock1.Release())
		})

		lock2, err := locker.AcquireRead("/rw-multi-reader-block", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock2.Release())
		})

		writeLock, err := locker.AcquireWrite("/rw-multi-reader-block", 3)
		require.Error(t, err)
		assertAlreadyLocked(t, err)
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
		assertWaitAround(t, t2.Sub(t1))
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
		assertWaitAround(t, t2.Sub(t1))
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
		assertWaitAround(t, t2.Sub(t1))
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

		waitUntilLegacyWriterQueued(t, "/rw-pending-legacy-writer")

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

		waitUntilLegacyWriterQueued(t, "/rw-many-readers-legacy-writer")

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

func TestRWLockKeyspaceIsolation(t *testing.T) {
	// These regressions are easy to miss because they depend on specific user
	// lock names. The rule is simple: internal RW metadata paths must never
	// change the behavior of unrelated user-visible legacy lock keys.

	t.Run("writer intents stay internal", func(t *testing.T) {
		legacyLocker := testLegacyLocker()

		// This uses WaitAcquire on purpose because waiting writers need extra RW
		// bookkeeping. Older versions did not reserve any extra user-visible key
		// names, so "/rw-keyspace-intent" and
		// "/rw-keyspace-intent.__rwlock-writer-intents" must remain unrelated
		// legacy keys even after RW support is added.
		waitingWriter, err := legacyLocker.WaitAcquire("/rw-keyspace-intent", 3)
		require.NoError(t, err)
		require.NotNil(t, waitingWriter)
		t.Cleanup(func() {
			require.NoError(t, waitingWriter.Release())
		})

		// This second key intentionally matches the suffix that previously leaked
		// into the public keyspace. It must still behave like an ordinary,
		// independent legacy lock key.
		independentLock, err := legacyLocker.Acquire("/rw-keyspace-intent.__rwlock-writer-intents", 3)
		require.NoError(t, err)
		require.NotNil(t, independentLock)
		require.NoError(t, independentLock.Release())
	})

	t.Run("reader entries stay internal", func(t *testing.T) {
		rwLocker := testRWLocker()
		legacyLocker := testLegacyLocker()

		// This key name matches the reader subtree shape that previously leaked
		// into the public keyspace. A reader on "/rw-keyspace-readers" must not
		// accidentally reserve or block the legacy key
		// "/rw-keyspace-readers/__rwlock-readers".
		readLock, err := rwLocker.AcquireRead("/rw-keyspace-readers", 3)
		require.NoError(t, err)
		require.NotNil(t, readLock)
		t.Cleanup(func() {
			require.NoError(t, readLock.Release())
		})

		// If this acquisition is blocked by the active reader above, RW metadata is
		// still leaking into the public legacy namespace.
		independentLock, err := legacyLocker.Acquire("/rw-keyspace-readers/__rwlock-readers", 3)
		require.NoError(t, err)
		require.NotNil(t, independentLock)
		require.NoError(t, independentLock.Release())
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
		assertWaitAround(t, t2.Sub(t1))
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
		assertWaitAround(t, t2.Sub(t1))
	})
}

func TestRWLockTimeouts(t *testing.T) {
	t.Run("wait acquire read stops at max try lock timeout behind a legacy writer", func(t *testing.T) {
		legacyLocker := testLegacyLocker()
		rwLocker := testRWLocker(WithMaxTryLockTimeout(time.Second))

		lock, err := legacyLocker.Acquire("/legacy-write-read-timeout", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock.Release())
		})

		t1 := time.Now()
		readLock, err := rwLocker.WaitAcquireRead("/legacy-write-read-timeout", 3)
		t2 := time.Now()

		require.Error(t, err)
		assertAlreadyLocked(t, err)
		assert.Nil(t, readLock)
		assertTimeoutAround(t, t2.Sub(t1), time.Second)
	})

	t.Run("legacy wait acquire stops at max try lock timeout behind an rw reader", func(t *testing.T) {
		legacyLocker := testLegacyLocker(WithMaxTryLockTimeout(time.Second))
		rwLocker := testRWLocker()

		lock, err := rwLocker.AcquireRead("/rw-read-legacy-timeout", 3)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lock.Release())
		})

		t1 := time.Now()
		writeLock, err := legacyLocker.WaitAcquire("/rw-read-legacy-timeout", 3)
		t2 := time.Now()

		require.Error(t, err)
		assertAlreadyLocked(t, err)
		assert.Nil(t, writeLock)
		assertTimeoutAround(t, t2.Sub(t1), time.Second)
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

	t.Run("release a nil rw lock should panic", func(t *testing.T) {
		var lock *EtcdRWLock
		require.PanicsWithValue(t, "nil rw lock", func() {
			_ = lock.Release()
		})
	})

	t.Run("failed release does not mark the read lock as released", func(t *testing.T) {
		cli := client()
		cli.Close()

		lock := &EtcdRWLock{
			Mutex:   &sync.Mutex{},
			client:  cli,
			lockKey: "/rw-release-delete-error",
		}

		err := lock.Release()
		require.Error(t, err)
		require.False(t, lock.released)
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

func waitUntilLegacyWriterQueued(t *testing.T, key string) {
	t.Helper()

	resourceKey := addPrefix(key)
	queuePrefix := rwQueuePrefix(resourceKey)
	readerPrefix := rwReadersPrefix(resourceKey)
	cli := client()
	defer cli.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.Get(t.Context(), queuePrefix, etcdv3.WithPrefix())
		require.NoError(t, err)
		for _, kv := range resp.Kvs {
			if !strings.HasPrefix(string(kv.Key), readerPrefix) {
				return
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("legacy writer was never observed in queue for key %q", key)
}

func assertAlreadyLocked(t *testing.T, err error) {
	t.Helper()

	var lockErr *ErrAlreadyLocked
	assert.True(t, errors.As(err, &lockErr))
}

func assertWaitAround(t *testing.T, duration time.Duration) {
	t.Helper()

	require.GreaterOrEqual(t, duration, 500*time.Millisecond)
	require.Less(t, duration, 2500*time.Millisecond)
}

func assertTimeoutAround(t *testing.T, duration time.Duration, expected time.Duration) {
	t.Helper()

	require.GreaterOrEqual(t, duration, expected-200*time.Millisecond)
	require.Less(t, duration, expected+1500*time.Millisecond)
}
