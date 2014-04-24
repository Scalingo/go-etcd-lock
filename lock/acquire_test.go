package lock

import (
	"testing"
	"time"

	"github.com/juju/errgo"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAcquire(t *testing.T) {
	Convey("A lock shouldn't be acquired twice", t, func() {
		lock, err := Acquire(client(), "/lock", 10)
		defer lock.Release()
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)
		lock, err = Acquire(client(), "/lock", 10)
		So(lock, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(errgo.Cause(err), ShouldHaveSameTypeAs, &Error{})
	})

	Convey("After expiration, a lock should be acquirable again", t, func() {
		lock, err := Acquire(client(), "/lock-expire", 1)
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)

		time.Sleep(2 * time.Second)

		lock, err = Acquire(client(), "/lock-expire", 1)
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)
		lock.Release()
	})
}
