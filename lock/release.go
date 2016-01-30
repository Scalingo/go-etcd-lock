package lock

import (
	ect "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"gopkg.in/errgo.v1"
)

func (l *EtcdLock) Release() error {
	if l == nil {
		return errgo.New("nil lock")
	}

	kapi := ect.NewKeysAPI(*l.client)
	_, err := kapi.Delete(context.Background(), l.key, &ect.DeleteOptions{Recursive: false})
	if err != nil {
		return errgo.Mask(err)
	}

	return nil
}
