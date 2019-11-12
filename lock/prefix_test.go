package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddPrefix(t *testing.T) {
	t.Run("addPrefix should add the prefix to the user key", func(t *testing.T) {
		t.Run("/key should become "+prefix+"/key", func(t *testing.T) {
			assert.Equal(t, addPrefix("/key"), "/etcd-lock/key")
		})
		t.Run("If the slash is missing, it should be added", func(t *testing.T) {
			assert.Equal(t, addPrefix("key"), "/etcd-lock/key")
		})
	})
}
