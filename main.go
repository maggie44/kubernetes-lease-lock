package leaselock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	coordinationv1 "k8s.io/api/coordination/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordinationclientv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/utils/ptr"
)

// Locker implements the Locker interface using the kubernetes Lease resource.
type Locker struct {
	// holderIdentity is a unique ID for the client acquiring the lock
	holderIdentity string
	// LeaseClient is the client used to interact with the Lease resource
	leaseClient coordinationclientv1.LeaseInterface
	// LeaseDuration is the duration a lock can exist before it can be forcibly acquired by another client
	LeaseDuration time.Duration
	// goroutineCancel context for canceling goroutine
	goroutineCancel context.CancelFunc
	// name is the name of the Lease resource. Only one person can use a Lease of the same name at a time.
	name string
	// retryWait is the duration the Lock function will wait before retrying after failing to acquire the lock
	retryWait time.Duration
	// A wait group used to ensure the goroutine has exited before attempting the unlock. This prevents any
	// risk of a race condition
	wg sync.WaitGroup
}

// NewLocker creates a Locker.
func NewLocker(
	ctx context.Context,
	client kubernetes.Interface,
	name string,
	holderIdentity string,
	namespace string,
) (*Locker, error) {
	// Create the Locker
	locker := &Locker{
		name: name,
		// Use the identifiable hostname, but add a random string as this needs to be unique to stop
		// other requests from being able to call the unlock function
		holderIdentity: holderIdentity + uuid.NewString(),
		// Time to wait between requests for the lock.
		retryWait: time.Second * 3,
		// LeaseDuration is the amount of time before the lease expires. Renewal will take place at half this time.
		LeaseDuration: 10 * time.Second,
		// Create the Lease client
		leaseClient: client.CoordinationV1().Leases(namespace),
	}

	// Create the Lease if it doesn't exist
	_, err := locker.leaseClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return nil, err
		}

		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: coordinationv1.LeaseSpec{
				LeaseTransitions: ptr.To(int32(0)),
			},
		}

		_, err := locker.leaseClient.Create(ctx, lease, metav1.CreateOptions{})
		if err != nil {
			// If the lease was created by another process after the Get call but before the Create call,
			// the error will be "AlreadyExists". In this case, we can simply continue with the existing lease.
			if !k8serrors.IsAlreadyExists(err) {
				return nil, err
			}
		}
	}

	return locker, nil
}

// Lock is responsible for acquiring the lock using the Kubernetes Lease resource. It
// uses a loop (block) to continuously try to acquire the lock until it succeeds. The
// caller must call Unlock to release the lock.
func (l *Locker) Lock(ctx context.Context) error {
	// Block until we get a lock
	for {
		// Get the Lease from Kubernetes
		lease, err := l.leaseClient.Get(ctx, l.name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("could not get lease resource for lock: %w", err)
		}

		// Check if the lock is already held and if it is, check to see if the lock
		// has expired. If it has not expired, wait for retryWait before trying again.
		if lease.Spec.HolderIdentity != nil {
			// Get the renew time from the Lease
			if lease.Spec.RenewTime == nil {
				// Avoid risk of a panic by returning an error
				return errors.New("lease renew time is nil")
			}

			// LeaseDuration as set via LeaseDuration at initiation
			if lease.Spec.LeaseDurationSeconds == nil {
				// Avoid risk of a panic by returning an error
				return errors.New("lease duration is nil")
			}

			// Convert the int32 duration in to a time.Duration
			leaseDuration := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
			// If RenewTime+leaseDuration is after Now then return true
			if lease.Spec.RenewTime.Time.Add(leaseDuration).After(time.Now()) {
				// The lock is already held and hasn't expired yet. Will
				// wait for retryWait before looping to try again.
				time.Sleep(l.retryWait)

				continue
			}
		}

		// Nobody holds the lock, try and lock it
		lease.Spec.HolderIdentity = ptr.To(l.holderIdentity)

		// Increment the lease transitions
		lease.Spec.LeaseTransitions = ptr.To((*lease.Spec.LeaseTransitions) + 1)

		// Set the acquire time
		lease.Spec.AcquireTime = &metav1.MicroTime{Time: time.Now()}

		// Set the renew time to now
		lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}

		// Set the LeaseDuration if it is greater than 0
		lease.Spec.LeaseDurationSeconds = ptr.To(int32(l.LeaseDuration.Seconds()))

		// Update the Lease
		_, err = l.leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
		if err == nil {
			// We got the lock, break the loop
			slog.Debug("lease lock acquired")
			break
		}

		// If the error isn't a conflict then something went wrong
		if !k8serrors.IsConflict(err) {
			return fmt.Errorf("error trying to update Lease: %w", err)
		}

		// If it is a conflict, another client beat us to the lock since we
		// fetched it. Waiting before trying again.
		select {
		case <-time.After(l.retryWait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Set the goroutine cancel context in the Locker so it can be called by the Unlock function
	var goroutineCtx context.Context
	goroutineCtx, l.goroutineCancel = context.WithCancel(ctx)
	// Storing the waitgroup so we can wait in the unlock function.
	l.wg.Add(1)
	// Start a goroutine to renew the lock
	go func() {
		defer l.goroutineCancel()
		defer l.wg.Done()

		// Create a ticker to renew the lock. We have only just acquired the lock
		// so we do not need to renew immediately. We can wait for the first tick.
		renewDuration := l.LeaseDuration / 2
		timer := time.NewTimer(renewDuration)

		for {
			// Wait for the next tick or context cancellation
			select {
			case <-goroutineCtx.Done():
				return
			case <-timer.C:
				// Renew the lock immediately before waiting for the first tick.
				// If the context is cancelled then we can stop the goroutine
				// as the user has requested this stop.
				err := renewLock(goroutineCtx, l)
				if err != nil && !errors.Is(err, context.Canceled) {
					// Logging here instead of using an error channel as we have returned
					// from the broader function already to allow the user to continue.
					slog.Error("error renewing lock", "error", err)

					return
				}

				// Reset the timer to half the lease duration
				timer.Reset(renewDuration)
			}
		}
	}()

	return nil
}

// renewLock is used to renew a lock by updating the lease's renew time if the lock is
// held and the caller is the lock holder.
func renewLock(goroutineCtx context.Context, l *Locker) error {
	// Get the Lease
	lease, err := l.leaseClient.Get(goroutineCtx, l.name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Check if the lock is already held
	if lease.Spec.HolderIdentity == nil {
		return errors.New("lock is no longer held")
	}

	// Check if we are the lock holder
	if *lease.Spec.HolderIdentity != l.holderIdentity {
		return errors.New("no longer the lock holder")
	}

	// Set the renew time to now
	lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}

	// Update the Lease.
	_, err = l.leaseClient.Update(goroutineCtx, lease, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// Unlock function is responsible for releasing the lock held by the client.
func (l *Locker) Unlock() error {
	// Stop the lock renewal goroutine
	l.goroutineCancel()

	// Wait for the renew goroutine to exit. This ensures there is no race condition.
	// If an Update had already been called then the cancellation of the context may
	// not abort the request.
	l.wg.Wait()

	// Build a timeout cancel. We do now want to use a context from the main process
	// as we always want the unlock to occur at the end of the calling function.
	// Adding a timeout prevents locks from being held indefinitely. We use the
	// l.leaseDuration as the timeout as this is the maximum time a lock can be held
	// anyway, so after this amount of time there is no need trying to remove it.
	ctx, cancel := context.WithTimeout(context.Background(), l.LeaseDuration)
	defer cancel()

	// Get the Lease
	lease, err := l.leaseClient.Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not get lease resource for lock: %w", err)
	}

	// The holder has to have a value
	if lease.Spec.HolderIdentity == nil {
		return errors.New("no lock holder value")
	}

	// Check if we are the lock holder
	if *lease.Spec.HolderIdentity != l.holderIdentity {
		return errors.New("not the lock holder")
	}

	// Clear the holder and the acquire time
	lease.Spec.HolderIdentity = nil
	lease.Spec.AcquireTime = nil
	lease.Spec.LeaseDurationSeconds = nil
	lease.Spec.RenewTime = nil

	// Update the Lease
	_, err = l.leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("unlock: error when trying to update Lease: %w", err)
	}

	slog.Debug("lease lock released")

	return nil
}
