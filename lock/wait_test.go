package lock

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWait(t *testing.T) {
	locker := NewEtcdLocker(client())
	Convey("Wait should wait the end of a lock", t, func() {
		t1 := time.Now()
		_, err := locker.Acquire("/lock-wait", 2)
		So(err, ShouldBeNil)

		err = locker.Wait("/lock-wait")
		So(err, ShouldBeNil)

		t2 := time.Now()
		So(int(t2.Sub(t1).Seconds()), ShouldEqual, 2)
	})

	Convey("Wait should return directly with an unlocked key", t, func() {
		t1 := time.Now()

		err := locker.Wait("/lock-free-wait")
		So(err, ShouldBeNil)

		t2 := time.Now()
		So(int(t2.Sub(t1).Seconds()), ShouldEqual, 0)
	})
}
