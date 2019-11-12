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
