package lock

import (
	"context"
	stdErrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWait(t *testing.T) {
	locker := NewEtcdLocker(client())
	t.Run("Wait should wait the end of a lock", func(t *testing.T) {
		l, err := locker.WaitAcquire("/lock-wait", 3)
		require.NoError(t, err)
		assert.NotNil(t, l)

		t1 := time.Now()
		err = locker.Wait("/lock-wait")
		require.NoError(t, err)
		t2 := time.Now()

		assert.Equal(t, 3, int(t2.Sub(t1).Seconds()))
	})

	t.Run("Wait should return directly with an unlocked key", func(t *testing.T) {
		t1 := time.Now()

		err := locker.Wait("/lock-free-wait")
		require.NoError(t, err)

		t2 := time.Now()
		assert.Equal(t, 0, int(t2.Sub(t1).Seconds()))
	})

	t.Run("WaitWithContext should return directly with an unlocked key and release the lock", func(t *testing.T) {
		err := locker.WaitWithContext(context.Background(), "/lock-free-wait-context")
		require.NoError(t, err)

		lock, err := locker.Acquire("/lock-free-wait-context", 1)
		require.NoError(t, err)
		require.NoError(t, lock.Release())
	})

	t.Run("WaitWithContext should fail immediately when the context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		t1 := time.Now()
		err := locker.WaitWithContext(ctx, "/lock-wait-context-canceled")
		t2 := time.Now()

		require.Error(t, err)
		assert.True(t, stdErrors.Is(err, context.Canceled))
		assert.Less(t, t2.Sub(t1), 100*time.Millisecond)
	})

	t.Run("WaitWithContext should stop when the context deadline is exceeded", func(t *testing.T) {
		locker := NewEtcdLocker(
			client(),
			WithTryLockTimeout(time.Second),
			WithMaxTryLockTimeout(5*time.Second),
			WithCooldownTryLockDuration(10*time.Millisecond),
		)
		firstLock, err := locker.Acquire("/lock-wait-context-timeout", 10)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, firstLock.Release())
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		t1 := time.Now()
		err = locker.WaitWithContext(ctx, "/lock-wait-context-timeout")
		t2 := time.Now()

		require.Error(t, err)
		assert.True(t, stdErrors.Is(err, context.DeadlineExceeded))
		assert.Less(t, t2.Sub(t1), 500*time.Millisecond)
	})
}
