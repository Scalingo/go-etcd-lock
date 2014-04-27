v0.1

```go
func Acquire(client *etcd.Client, key string, uint64 ttl) (*Lock, error)
func (lock *Lock) Release() error
```

v0.2

```go
func Wait(client *etcd.Client, key string) error
func WaitAcquire(client *etcd.Client, key string, uint64 ttl) (*Lock, erro)
```


