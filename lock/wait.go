package lock

import "context"

func (locker *EtcdLocker) Wait(key string) error {
	return locker.WaitWithContext(context.Background(), key)
}

func (locker *EtcdLocker) WaitWithContext(ctx context.Context, key string) error {
	lock, err := locker.WaitAcquireWithContext(ctx, key, 1)
	if err != nil {
		return err
	}
	err = lock.Release()
	if err != nil {
		return err
	}
	return nil
}
