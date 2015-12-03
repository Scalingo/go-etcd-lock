package lock

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/errgo.v1"
)

func TestAcquire(t *testing.T) {
	locker := NewEtcdLocker(client())
	Convey("A lock shouldn't be acquired twice", t, func() {
		lock, err := locker.Acquire("/lock", 10)
		defer lock.Release()
		So(err, ShouldBeNil)
		So(lock, ShouldNotBeNil)
		lock, err = locker.Acquire("/lock", 10)
		So(err, ShouldNotBeNil)
		So(errgo.Cause(err), ShouldHaveSameTypeAs, &Error{})
		So(lock, ShouldBeNil)
	})

	Convey("After expiration, a lock should be acquirable again", t, func() {
		lock, err := locker.Acquire("/lock-expire", 1)
		So(err, ShouldBeNil)
		So(lock, ShouldNotBeNil)

		time.Sleep(2 * time.Second)

		lock, err = locker.Acquire("/lock-expire", 1)
		So(err, ShouldBeNil)
		So(lock, ShouldNotBeNil)
		lock.Release()
	})
}

func TestWaitAcquire(t *testing.T) {
	locker := NewEtcdLocker(client())
	Convey("WaitLock should lock a key when the key is free", t, func() {
		Convey("It should wait when a key is locked", func() {
			lock, err := locker.Acquire("/lock-wait-acquire", 2)
			So(err, ShouldBeNil)
			So(lock, ShouldNotBeNil)

			t1 := time.Now()
			lock, err = locker.WaitAcquire("/lock-wait-acquire", 2)
			t2 := time.Now()

			So(err, ShouldBeNil)
			So(lock, ShouldNotBeNil)
			So(int(t2.Sub(t1).Seconds()), ShouldEqual, 2)

			lock.Release()
		})

		Convey("It should not wait if key is free", func() {
			t1 := time.Now()
			lock, err := locker.WaitAcquire("/lock-wait-acquire-free", 2)
			t2 := time.Now()

			So(err, ShouldBeNil)
			So(lock, ShouldNotBeNil)
			So(int(t2.Sub(t1).Seconds()), ShouldEqual, 0)
		})
	})

	locker.Acquire("/lock-double-wait", 2)
	Convey("When two instances are waiting for a 2 seconds lock", t, func() {
		t1 := time.Now()
		ends := make(chan time.Time)
		locks := make(chan Lock)
		errs := make(chan error)
		Convey("The first request should wait 2 and the 2nd, 4 seconds", func() {
			go func() {
				lock, err := locker.WaitAcquire("/lock-double-wait", 2)
				errs <- err
				locks <- lock
				ends <- time.Now()
			}()
			go func() {
				lock, err := locker.WaitAcquire("/lock-double-wait", 2)
				errs <- err
				locks <- lock
				ends <- time.Now()
			}()

			err := <-errs
			lock := <-locks
			So(err, ShouldBeNil)
			So(lock, ShouldNotBeNil)
			t2 := <-ends
			So(int(t2.Sub(t1).Seconds()), ShouldEqual, 2)

			err = <-errs
			lock = <-locks
			So(err, ShouldBeNil)
			So(lock, ShouldNotBeNil)
			t2 = <-ends
			So(int(t2.Sub(t1).Seconds()), ShouldEqual, 4)
		})
	})
}
