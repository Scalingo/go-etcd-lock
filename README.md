# etcd-lock v5.0.4

[ ![Codeship Status for Scalingo/go-etcd-lock](https://app.codeship.com/projects/fda40030-9bc6-0135-f438-2e7abb19bcf1/status?branch=master)](https://app.codeship.com/projects/252772)

## Import

```
# Master via standard import
get get github.com/Scalingo/go-etcd-lock

# Last stable is v0 via gopkg.in
go get gopkg.in/Scalingo/go-etcd-lock.v3vendor/github.com/Scalingo/go-etcd-lock/lock/lock
```

## Example

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

## Testing

You need a etcd instance running on `localhost:2379`, then:

```
go test ./...
```

## Generate mock

From the `/lock/` folder:

```
mockgen -destination lockmock/gomock_locker.go -package lockmock github.com/Scalingo/go-etcd-lock/lock Locker
mockgen -destination lockmock/gomock_lock.go -package lockmock github.com/Scalingo/go-etcd-lock/lock Lock
```

## Release a New Version

Bump new version number in `CHANGELOG.md` and `README.md`.

Commit, tag and create a new release:

```sh
git add CHANGELOG.md README.md
git commit -m "Bump v5.0.4"
git tag v5.0.4
git push origin master
git push --tags
hub release create v5.0.4
```

The title of the release should be the version number and the text of the release is the same as the changelog.
