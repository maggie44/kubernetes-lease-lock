package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	leaselock "github.com/maggie44/kubernetes-lease-lock"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func main() {
	err := lock()
	if err != nil {
		slog.Error("error obtaining lock", "error", err)
	}
}

func lock() error {
	const lockName = "my-lock"
	const lockNamespace = "default"
	const holderIdentity = "my-unique-identifier" // Consider using an environment variable from the container, such as POD_NAME.

	// This is a fake clientset that can be used for testing. Replace this with your real Kubernetes clientset. Example below.
	client := fake.NewSimpleClientset()

	// Use the below instead of the fake clientset above if running inside a real pod on Kubernetes.
	// client, err := inClusterClientset()
	// if err != nil {
	// 	return err
	// }

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

	return nil
}

// inClusterConfig configures the Kubernetes client assuming it is running inside a pod
func inClusterClientset() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}
