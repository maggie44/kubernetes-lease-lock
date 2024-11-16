# kubernetes-lease-lock

Golang implementation of a Kubernetes lease lock. See below, and/or `example` folder.

```golang
// `kubernetesClient` is a kubernetes clientset (*kubernetes.Clientset).
// See example folder for how to create a clientset.
//
// `lockName` is the name of the lock. A user trying to acquire a lock
// of the same name in the same namespace will be required to wait.
//
// `holderIdentity` is the identity of the holder of the lock. This is
// used to identify the holder of the lock but does not impact the ability
// to acquire the lock.
//
// `lockNamespace` is the namespace in which the lock will be created. A
// lock created with the same name in different namespaces is not considered
// the same lock. If in doubt, use `default` as the namespace name.
locker, err := leaselock.NewLocker(
    context.Background(),
    client,
    lockName,
    holderIdentity,
    lockNamespace,
)
if err != nil {
    return err
}

// Create a context with timeout that specifies how long to keep trying to obtain a lock.
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// This will block until the lock is acquired or the context is cancelled.
err = locker.Lock(ctx)
if err != nil {
    return err
}

// Do things here while holding the lock.
fmt.Println("I have the lock!")

// Release the lock.
err = locker.Unlock()
if err != nil {
    return err
}

// Alternatively, use a defer statement immediately after `locker.Lock`
// to ensure the lock is released.
/*
    err = locker.Lock(ctx)
    if err != nil {
        return err
    }
    defer func() {
        err := locker.Unlock()
        if err != nil {
            slog.Error("error unlocking the lock", "error", err)
        }
    }()
*/
```
