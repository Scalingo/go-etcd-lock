package lock

import (
	"testing"
	"time"
	. "github.com/smartystreets/goconvey/convey"
)

func TestWait(t *testing.T) {
	Convey("Wait should wait the end of a lock", t, func() {
		t1 := time.Now()
		_, err := Acquire(client(), "/lock-wait", 2)
		So(err, ShouldBeNil)

		err = Wait(client(), "/lock-wait")
		So(err, ShouldBeNil)

		t2 := time.Now()
		So(int(t2.Sub(t1).Seconds()), ShouldEqual, 2)
	})

	Convey("Wait should return directly with an unlocked key", t, func() {
		t1 := time.Now()

		err := Wait(client(), "/lock-free-wait")
		So(err, ShouldBeNil)

		t2 := time.Now()
		So(int(t2.Sub(t1).Seconds()), ShouldEqual, 0)
	})
}
