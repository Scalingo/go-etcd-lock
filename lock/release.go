package lock

import (
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"gopkg.in/errgo.v1"
)

func (l *EtcdLock) Release() error {
	if l == nil {
		return errgo.New("nil lock")
	}

	_, err := l.kapi.Delete(context.Background(), l.key, &etcd.DeleteOptions{})
	if err != nil {
		return errgo.Mask(err)
	}

	return nil
}
