package lock

import (
	"testing"
	"time"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRelease(t *testing.T) {
	Convey("After release a key should be lockable immediately", t, func() {
		lock, err := Acquire(client(), "/lock-release", 10)
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)

		err = lock.Release()
		So(err, ShouldBeNil)

		lock, err = Acquire(client(), "/lock-release", 10)
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)

		err = lock.Release()
		So(err, ShouldBeNil)
	})

	Convey("After expiration, release a lock shouldn't produce an error", t, func() {
		lock, _ := Acquire(client(), "/lock-release-exp", 1)
		time.Sleep(2)
		err := lock.Release()
		So(err, ShouldBeNil)
	})
}
