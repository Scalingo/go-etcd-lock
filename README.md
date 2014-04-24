etcd-lock
=========

For version 0.4.0 of etcd the module lock has been deprecated.

This is a basic client implementation of lock based on the logics in mod/lock

This library doesn't provide the `*etcd.Client` because it doesn't want to
manage the condfiguration of it (TLS or not, endpoints etc.) So a client has to
exist previously

```go
l, err := lock.Acquire(client, "/name", 60)
if lockErr, ok := err.(*lock.Error); ok {
  // Key already locked
  fmt.Println(lockErr)
  return
} else if err != nil {
  // Communication with etcd has failed or other error
  panic(err)
}

// It's ok, lock is granted for 60 secondes

// When the opration is done we release the lock
err = l.Release()
if err != nil {
  // Something wrong can happen during release: connection problem with etcd
  panic(err)
}
```
