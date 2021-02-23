package lock

import "context"

func (locker *EtcdLocker) Wait(ctx context.Context, key string) error {
	lock, err := locker.WaitAcquire(ctx, key, 1)
	if err != nil {
		return err
	}
	err = lock.Release()
	if err != nil {
		return err
	}
	return nil
}
