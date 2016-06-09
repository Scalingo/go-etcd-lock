package lock

import (
	ect "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"gopkg.in/errgo.v1"
	"sort"
)

func (locker *EtcdLocker) Wait(key string) error {
	key = addPrefix(key)
	kapi := ect.NewKeysAPI(*locker.client)
	ctx := context.Background()

	for {
		res, err := kapi.Get(ctx, key, &ect.GetOptions{Recursive: true, Sort: true})
		if err != nil {
			// if key not found, lock is free
			if ect.IsKeyNotFound(err) {
				break
			}
			return errgo.Mask(err)
		}

		if len(res.Node.Nodes) == 0 {
			break
		}

		sort.Sort(res.Node.Nodes)
		currentLock := res.Node.Nodes[0]

		watcher := kapi.Watcher(currentLock.Key, &ect.WatcherOptions{AfterIndex: currentLock.CreatedIndex, Recursive: false})
		_, err = watcher.Next(ctx)
		if err != nil {
			return errgo.Mask(err)
		}
	}

	return nil
}
