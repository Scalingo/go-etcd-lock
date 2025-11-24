# etcd-lock v5.0.8

## Import

```
# Master via standard import
go get github.com/Scalingo/go-etcd-lock

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
version="5.0.8"

git switch --create release/${version}
git add CHANGELOG.md README.md
git commit --message="Bump v${version}"
git push --set-upstream origin release/${version}
gh pr create --reviewer=EtienneM --title "$(git log -1 --pretty=%B)"
```

Once the pull request merged, you can tag the new release.

```sh
git tag v${version}
git push origin master v${version}
gh release create v${version}
```

The title of the release should be the version number and the text of the release is the same as the changelog.
