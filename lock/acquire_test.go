package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gopkg.in/errgo.v1"
)

func TestAcquire(t *testing.T) {
	t.Run("A lock shouldn't be acquired twice", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTrylockTimeout(500*time.Millisecond))
		lock, err := locker.Acquire("/lock", 10)
		require.NoError(t, err)
		defer lock.Release()

		assert.NotNil(t, lock)
		lock, err = locker.Acquire("/lock", 10)
		assert.NotNil(t, err)
		assert.IsType(t, &Error{}, errgo.Cause(err))
		assert.Nil(t, lock)
	})

	t.Run("After expiration, a lock should be acquirable again", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTrylockTimeout(500*time.Millisecond))
		_, err := locker.Acquire("/lock-expire", 1)
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		locker = NewEtcdLocker(client(), WithTrylockTimeout(500*time.Millisecond))
		lock, err := locker.Acquire("/lock-expire", 1)
		require.NoError(t, err)
		lock.Release()
	})
}

func TestWaitAcquire(t *testing.T) {
	t.Run("WaitLock should lock a key when the key is free", func(t *testing.T) {
		t.Run("It should wait when a key is locked", func(t *testing.T) {
			locker := NewEtcdLocker(client(), WithTrylockTimeout(500*time.Millisecond))
			lock, err := locker.Acquire("/lock-wait-acquire", 2)
			require.NoError(t, err)
			assert.NotNil(t, lock)

			t1 := time.Now()
			lock, err = locker.WaitAcquire("/lock-wait-acquire", 2)
			t2 := time.Now()

			require.NoError(t, err)
			assert.NotNil(t, lock)
			assert.Equal(t, int(t2.Sub(t1).Seconds()), 2)

			lock.Release()
		})

		t.Run("It should not wait if key is free", func(t *testing.T) {
			locker := NewEtcdLocker(client(), WithTrylockTimeout(500*time.Millisecond))
			t1 := time.Now()
			lock, err := locker.WaitAcquire("/lock-wait-acquire-free", 2)
			t2 := time.Now()

			require.NoError(t, err)
			assert.NotNil(t, lock)
			assert.Equal(t, int(t2.Sub(t1).Seconds()), 0)
		})
	})

	t.Run("When two instances are waiting for a 2 seconds lock", func(t *testing.T) {
		locker := NewEtcdLocker(client(), WithTrylockTimeout(500*time.Millisecond))
		locker.Acquire("/lock-double-wait", 2)
		t1 := time.Now()
		ends := make(chan time.Time)
		locks := make(chan Lock)
		errs := make(chan error)

		t.Run("The first request should wait 2 and the 2nd, 4 seconds", func(t *testing.T) {
			go func(t *testing.T) {
				lock, err := locker.WaitAcquire("/lock-double-wait", 2)
				errs <- err
				locks <- lock
				ends <- time.Now()
			}(t)
			go func(t *testing.T) {
				lock, err := locker.WaitAcquire("/lock-double-wait", 2)
				errs <- err
				locks <- lock
				ends <- time.Now()
			}(t)

			err := <-errs
			lock := <-locks
			require.NoError(t, err)
			assert.NotNil(t, lock)
			t2 := <-ends
			assert.Equal(t, int(t2.Sub(t1).Seconds()), 2)

			err = <-errs
			lock = <-locks
			require.NoError(t, err)
			assert.NotNil(t, lock)
			t2 = <-ends
			assert.Equal(t, int(t2.Sub(t1).Seconds()), 4)
		})
	})
}
