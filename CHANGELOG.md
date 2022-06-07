# Changelog

## To be Released

* chore(go): use go 1.17
* Bump go.etcd.io/etcd/client/v3 from 3.5.0 to 3.5.4
* Bump github.com/stretchr/testify from 1.7.0 to 1.7.1

## v5.0.5

* Bump github.com/golang/mock from 1.4.4 to 1.6.0
* Bump go.etcd.io/etcd/client from 3.3.0 to 3.5.0 (with Go modules support! \o/)

## v5.0.4

* Bump github.com/golang/mock from 1.4.3 to 1.4.4 #12
* Bump github.com/stretchr/testify from 1.4.0 to 1.7.0 #13

## v5.0.3

* Use v5 version in test files

## v5.0.2

* Package is now github.com/Scalingo/go-etcd-lock/v5 conforming to go modules semantic

## v5.0.1

* Clean Go modules dependencies
* Checkin go.mod

## v5.0.0

* Migration to Go Modules
* Using `go.etcd.io/etcd/v3/...` instead of `go.etcd.io/etcd/...`
  Reason: etcd commit https://github.com/etcd-io/etcd/commit/96cce208c2cb9e70ac3573b3f76eed7c84d262d1

## v4.0.0

* Fix error management
* Fix connection leak to ETCD (`clientv3/concurrency.Session` was never closed)
* More go-ish API
  * `lock.Error` -> `lock.ErrAlreadyLocked`
  * `lock.WithTrylockTimeout` -> `lock.WithTryLockTimeout`
* Better configurability in `lock.NewEtcdLocker()`
  * `lock.WithMaxTryLockTimeout`: Set a max duration a caller can wait for a lock in `WaitAcquire` (default: 2 minutes)
  * `lock.WithCooldownTryLockDuration`: Set a duration between calls to ETCD to acquire a lock (default: 1 second)
* Prevent `clientv3/concurrency.Mutex.Lock(ctx)` to be called with an infinite `context.Background()` which was basically blocking forever.

## v3.4.2

* Simpler dependencies management

## v3.4.1

* Fix race condition in specs

## v3.4.0

* ETCD Client to v3.4.3

## v2.0

* Change ETCD client

## v0.2

```go
func Wait(client *etcd.Client, key string) error
func WaitAcquire(client *etcd.Client, key string, uint64 ttl) (*Lock, erro)
```


## v0.1

```go
func Acquire(client *etcd.Client, key string, uint64 ttl) (*Lock, error)
func (lock *Lock) Release() error
```
