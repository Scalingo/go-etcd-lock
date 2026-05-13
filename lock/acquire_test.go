package lock

import (
	"context"
	stdErrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Scalingo/go-utils/errors/v3"
)

func TestAcquire(t *testing.T) {
	t.Run("A lock shouldn't be acquired twice", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTryLockTimeout(500*time.Millisecond))
		firstLock, err := locker.Acquire("/lock", 10)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, firstLock.Release())
		})

		assert.NotNil(t, firstLock)
		lock, err := locker.Acquire("/lock", 10)
		require.Error(t, err)
		var lockErr *ErrAlreadyLocked
		assert.True(t, errors.As(err, &lockErr))
		assert.Nil(t, lock)
	})

	t.Run("After expiration, a lock should be acquirable again", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTryLockTimeout(500*time.Millisecond))
		_, err := locker.Acquire("/lock-expire", 1)
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		locker = NewEtcdLocker(client(), WithTryLockTimeout(500*time.Millisecond))
		lock, err := locker.Acquire("/lock-expire", 1)
		require.NoError(t, err)
		require.NoError(t, lock.Release())
	})

	t.Run("AcquireWithContext should acquire a free lock", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTryLockTimeout(500*time.Millisecond))

		lock, err := locker.AcquireWithContext(context.Background(), "/lock-acquire-context-free", 10)

		require.NoError(t, err)
		require.NotNil(t, lock)
		require.NoError(t, lock.Release())
	})

	t.Run("AcquireWithContext should fail immediately when the context is canceled", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTryLockTimeout(time.Second))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		t1 := time.Now()
		lock, err := locker.AcquireWithContext(ctx, "/lock-acquire-context-canceled", 10)
		t2 := time.Now()

		require.Error(t, err)
		assert.True(t, stdErrors.Is(err, context.Canceled))
		assert.Nil(t, lock)
		assert.Less(t, t2.Sub(t1), 100*time.Millisecond)
	})

	t.Run("AcquireWithContext should stop when the context deadline is exceeded", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTryLockTimeout(time.Second))
		firstLock, err := locker.Acquire("/lock-acquire-context-timeout", 10)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, firstLock.Release())
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		t1 := time.Now()
		lock, err := locker.AcquireWithContext(ctx, "/lock-acquire-context-timeout", 10)
		t2 := time.Now()

		require.Error(t, err)
		assert.True(t, stdErrors.Is(err, context.DeadlineExceeded))
		assert.Nil(t, lock)
		assert.Less(t, t2.Sub(t1), 500*time.Millisecond)
	})
}

func TestWaitAcquire(t *testing.T) {
	t.Run("WaitLock should lock a key when the key is free", func(t *testing.T) {
		t.Run("It should wait when a key is locked", func(t *testing.T) {
			locker := NewEtcdLocker(
				client(),
				WithTryLockTimeout(500*time.Millisecond),
				WithCooldownTryLockDuration(0),
			)
			initialLock, err := locker.Acquire("/lock-wait-acquire", 2)
			require.NoError(t, err)
			assert.NotNil(t, initialLock)

			t1 := time.Now()
			lock, err := locker.WaitAcquire("/lock-wait-acquire", 2)
			t2 := time.Now()

			require.NoError(t, err)
			assert.NotNil(t, lock)
			assert.Equal(t, 2, int(t2.Sub(t1).Seconds()))

			require.NoError(t, lock.Release())
		})

		t.Run("It should not wait if key is free", func(t *testing.T) {
			locker := NewEtcdLocker(client(), WithTryLockTimeout(500*time.Millisecond))
			t1 := time.Now()
			lock, err := locker.WaitAcquire("/lock-wait-acquire-free", 2)
			t2 := time.Now()

			require.NoError(t, err)
			assert.NotNil(t, lock)
			assert.Equal(t, 0, int(t2.Sub(t1).Seconds()))
			require.NoError(t, lock.Release())
		})

		t.Run("it should not wait more than the maxTryLockTimeout", func(t *testing.T) {
			locker := NewEtcdLocker(
				client(),
				WithTryLockTimeout(500*time.Millisecond),
				WithMaxTryLockTimeout(time.Second),
			)
			initialLock, err := locker.Acquire("/lock-wait-acquire-timeout", 2)
			require.NoError(t, err)
			assert.NotNil(t, initialLock)

			t1 := time.Now()
			lock, err := locker.WaitAcquire("/lock-wait-acquire-timeout", 2)
			t2 := time.Now()

			var lockErr *ErrAlreadyLocked
			assert.True(t, errors.As(err, &lockErr))
			assert.Nil(t, lock)
			assert.Equal(t, 1, int(t2.Sub(t1).Seconds()))
		})
	})

	t.Run("When two instances are waiting for a 2 seconds lock", func(t *testing.T) {
		locker := NewEtcdLocker(
			client(),
			WithTryLockTimeout(500*time.Millisecond),
			WithCooldownTryLockDuration(0),
		)
		_, err := locker.Acquire("/lock-double-wait", 2)
		require.NoError(t, err)
		t1 := time.Now()
		ends := make(chan time.Time)
		locks := make(chan Lock)
		errs := make(chan error)

		t.Run("The first request should wait 2 and the 2nd, 4 seconds", func(t *testing.T) {
			go func() {
				lock, err := locker.WaitAcquire("/lock-double-wait", 2)
				errs <- err
				locks <- lock
				ends <- time.Now()
			}()
			go func() {
				lock, err := locker.WaitAcquire("/lock-double-wait", 2)
				errs <- err
				locks <- lock
				ends <- time.Now()
			}()

			err := <-errs
			lock := <-locks
			require.NoError(t, err)
			assert.NotNil(t, lock)
			t2 := <-ends
			assert.Equal(t, 2, int(t2.Sub(t1).Seconds()))

			err = <-errs
			lock = <-locks
			require.NoError(t, err)
			assert.NotNil(t, lock)
			t2 = <-ends
			assert.Equal(t, 4, int(t2.Sub(t1).Seconds()))
			require.NoError(t, lock.Release())
		})
	})

	t.Run("WaitAcquireWithContext should fail immediately when the context is canceled", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTryLockTimeout(time.Second))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		t1 := time.Now()
		lock, err := locker.WaitAcquireWithContext(ctx, "/lock-wait-acquire-context-canceled", 10)
		t2 := time.Now()

		require.Error(t, err)
		assert.True(t, stdErrors.Is(err, context.Canceled))
		assert.Nil(t, lock)
		assert.Less(t, t2.Sub(t1), 100*time.Millisecond)
	})

	t.Run("WaitAcquireWithContext should stop when the context deadline is exceeded", func(t *testing.T) {
		locker := NewEtcdLocker(
			client(),
			WithTryLockTimeout(time.Second),
			WithMaxTryLockTimeout(5*time.Second),
			WithCooldownTryLockDuration(10*time.Millisecond),
		)
		firstLock, err := locker.Acquire("/lock-wait-acquire-context-timeout", 10)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, firstLock.Release())
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		t1 := time.Now()
		lock, err := locker.WaitAcquireWithContext(ctx, "/lock-wait-acquire-context-timeout", 10)
		t2 := time.Now()

		require.Error(t, err)
		assert.True(t, stdErrors.Is(err, context.DeadlineExceeded))
		assert.Nil(t, lock)
		assert.Less(t, t2.Sub(t1), 500*time.Millisecond)
	})
}
