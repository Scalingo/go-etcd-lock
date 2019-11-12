package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRelease(t *testing.T) {
	locker := NewEtcdLocker(client())
	t.Run("After release a key should be lockable immediately", func(t *testing.T) {
		lock, err := locker.Acquire("/lock-release", 10)
		assert.NotNil(t, lock)
		assert.NoError(t, err)

		err = lock.Release()
		assert.NoError(t, err)

		lock, err = locker.Acquire("/lock-release", 10)
		assert.NotNil(t, lock)
		assert.NoError(t, err)

		err = lock.Release()
		assert.NoError(t, err)
	})

	t.Run("After expiration, release a lock shouldn't produce an error", func(t *testing.T) {
		lock, _ := locker.Acquire("/lock-release-exp", 1)
		time.Sleep(2)
		err := lock.Release()
		assert.NoError(t, err)
	})

	t.Run("Release a nil lock should not panic", func(t *testing.T) {
		var lock *EtcdLock
		err := lock.Release()
		assert.NotNil(t, err)
	})
}
