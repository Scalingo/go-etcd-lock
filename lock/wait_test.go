package lock

import (
	"testing"
	"context"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWait(t *testing.T) {
	locker := NewEtcdLocker(client())
	t.Run("Wait should wait the end of a lock", func(t *testing.T) {
		l, err := locker.WaitAcquire(context.Background(),"/lock-wait", 3)
		require.NoError(t, err)
		assert.NotNil(t, l)

		t1 := time.Now()
		err = locker.Wait(context.Background(),"/lock-wait")
		require.NoError(t, err)
		t2 := time.Now()

		assert.Equal(t, int(t2.Sub(t1).Seconds()), 3)
	})

	t.Run("Wait should return directly with an unlocked key", func(t *testing.T) {
		t1 := time.Now()

		err := locker.Wait(context.Background(),"/lock-free-wait")
		require.NoError(t, err)

		t2 := time.Now()
		assert.Equal(t, int(t2.Sub(t1).Seconds()), 0)
	})
}
