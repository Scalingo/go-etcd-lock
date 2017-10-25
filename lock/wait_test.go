package lock

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWait(t *testing.T) {
	locker := NewEtcdLocker(client())
	Convey("Wait should wait the end of a lock", t, func() {
		l, err := locker.WaitAcquire("/lock-wait", 3)
		So(err, ShouldBeNil)
		So(l, ShouldNotBeNil)

		t1 := time.Now()
		err = locker.Wait("/lock-wait")
		So(err, ShouldBeNil)
		t2 := time.Now()

		So(int(t2.Sub(t1).Seconds()), ShouldEqual, 3)
	})

	Convey("Wait should return directly with an unlocked key", t, func() {
		t1 := time.Now()

		err := locker.Wait("/lock-free-wait")
		So(err, ShouldBeNil)

		t2 := time.Now()
		So(int(t2.Sub(t1).Seconds()), ShouldEqual, 0)
	})
}
