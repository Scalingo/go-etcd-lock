package lock

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRelease(t *testing.T) {
	locker := NewEtcdLocker(client())
	Convey("After release a key should be lockable immediately", t, func() {
		lock, err := locker.Acquire("/lock-release", 10)
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)

		err = lock.Release()
		So(err, ShouldBeNil)

		lock, err = locker.Acquire("/lock-release", 10)
		So(lock, ShouldNotBeNil)
		So(err, ShouldBeNil)

		err = lock.Release()
		So(err, ShouldBeNil)
	})

	Convey("After expiration, release a lock shouldn't produce an error", t, func() {
		lock, _ := locker.Acquire("/lock-release-exp", 1)
		time.Sleep(2)
		err := lock.Release()
		So(err, ShouldBeNil)
	})

	Convey("Release a nil lock should not panic", t, func() {
		var lock *EtcdLock
		err := lock.Release()
		So(err, ShouldNotBeNil)
	})
}
