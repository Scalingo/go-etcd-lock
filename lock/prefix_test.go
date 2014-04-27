package lock

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAddPrefix(t *testing.T) {
	Convey("addPrefix should add the prefix to the user key", t, func() {
		Convey("/key should become "+prefix+"/key", func() {
			So(addPrefix("/key"), ShouldEqual, "/etcd-lock/key")
		})
		Convey("If the slash is missing, it should be added", func() {
			So(addPrefix("key"), ShouldEqual, "/etcd-lock/key")
		})
	})
}
