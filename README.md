# etcd-lock v5.0.10

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

## Reader / Writer Lock

Use `NewEtcdRWLocker` when you want shared readers and exclusive writers without changing the existing lock behavior. RW bookkeeping lives in a private etcd namespace, while legacy and RW writers still share the same writer queue, so legacy locks and RW locks honor each other during a rollout.

During a migration from `go-etcd-lock` `v5.0.9` to `v6.*` you can safely run both implementations against the same lock key. Existing `EtcdLocker` locks remain write locks, `EtcdRWLocker.AcquireWrite` uses the same write-lock mechanism, and `EtcdRWLocker.AcquireRead` publishes reader state in private metadata. That means legacy writers still wait for active RW readers, new RW readers will not bypass older legacy writers already queued on the lock, and internal RW metadata does not leak into the public legacy keyspace.

```mermaid
sequenceDiagram
    participant R1 as RW reader #1
    participant E as etcd shared queue
    participant W1 as legacy writer
    participant R2 as later RW reader

    R1->>E: add reader entry
    Note over R1,E: reader is active

    W1->>E: add writer waiter / intent
    Note over W1,E: writer is queued behind active reader

    R2->>E: check queue for earlier writers
    E-->>R2: writer already queued
    Note over R2,E: R2 waits instead of bypassing W1

    R1->>E: release reader entry
    E-->>W1: writer reaches front of queue
    Note over W1,E: writer acquires lock

    W1->>E: release writer lock
    E-->>R2: no earlier writer remains
    Note over R2,E: reader can now enter
```

```go
locker := lock.NewEtcdRWLocker(client)

readLock, err := locker.AcquireRead("/name", 60)
if err != nil {
	panic(err)
}
defer readLock.Release()
```

```go
writeLock, err := locker.WaitAcquireWrite("/name", 60)
if err != nil {
	panic(err)
}
defer writeLock.Release()
```

## Read to Write Upgrade

If you acquire a read lock and later decide the resource must be changed, you can upgrade that read lock to a write lock with `(*lock.EtcdRWLock).Upgrade()` or `WaitUpgrade()`.

The upgrade is not atomic:
- writer intent is published first, so later readers stop entering
- the read lock is released
- the code then acquires the write lock

Because of that gap, you must re-read or re-validate the protected resource once the write lock has been granted, before applying any mutation such as repairing network state, updating configuration, or creating/deleting a dependent resource.

```mermaid
sequenceDiagram
    participant C as Caller
    participant R as RW read lock
    participant E as etcd queue
    participant W as Other readers/writers

    C->>R: AcquireRead()
    R->>E: register reader entry
    C->>C: inspect resource state

    alt resource must be updated
        C->>R: WaitUpgrade()
        R->>E: publish writer intent
        R->>E: remove reader entry
        Note over W,E: New readers stop entering
        W-->>E: existing readers/writers drain
        R->>E: acquire legacy write lock
        E-->>C: write lock granted
        C->>C: re-read / re-validate resource
        C->>C: apply mutation
    end
```

```go
readLock, err := locker.AcquireRead("/network", 60)
if err != nil {
	panic(err)
}

rwReadLock, ok := readLock.(*lock.EtcdRWLock)
if !ok {
	panic("unexpected lock type")
}

// Read and validate the resource first.
// If it needs to be updated, upgrade to a write lock.
writeLock, err := rwReadLock.WaitUpgrade()
if err != nil {
	panic(err)
}
defer writeLock.Release()

// Re-read or re-validate here before applying the update.
```

## Testing

You need a etcd instance running on `localhost:2379`, then:

```
go test ./...
```

## Release a New Version

Bump new version number in:
- `CHANGELOG.md`
- `README.md`
- `go.mod`, `mocks.json` and all imports in case of a new major version

Commit, tag and create a new release:

```sh
version="5.0.10"

git switch --create release/${version}
git add CHANGELOG.md README.md
git commit --message="Bump v${version}"
git push --set-upstream origin release/${version}
gh pr create --reviewer=Scalingo/team-ist --title "$(git log -1 --pretty=%B)"
```

Once the pull request merged, you can tag the new release.

```sh
git tag v${version}
git push origin master v${version}
gh release create v${version}
```

The title of the release should be the version number and the text of the release is the same as the changelog.
