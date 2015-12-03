package lock

import (
	"sort"

	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/errgo.v1"
)

func (locker *EtcdLocker) Wait(key string) error {
	key = addPrefix(key)

	for {
		res, err := locker.client.Get(key, true, true)
		if err != nil {
			etcdErr, ok := err.(*etcd.EtcdError)
			// if key not found, lock is free
			if ok && etcdErr.ErrorCode == 100 {
				break
			}
			return errgo.Mask(err)
		}

		if len(res.Node.Nodes) == 0 {
			break
		}

		sort.Sort(res.Node.Nodes)
		currentLock := res.Node.Nodes[0]

		_, err = locker.client.Watch(currentLock.Key, currentLock.CreatedIndex, false, nil, nil)
		if err != nil {
			return errgo.Mask(err)
		}
	}

	return nil
}
