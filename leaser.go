package leaser

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type driver interface {
	Tail(sinceRev, limit int64) ([]*Lease, error)
	Upsert(owner, resource string, dur time.Duration) error
	Delete(owner, resource string) error
}

type Leaser struct {
	d driver

	dur   time.Duration
	owner string

	leasesMux      *sync.RWMutex
	leases         map[string]*Lease
	leasesSinceRev int64

	wg      *sync.WaitGroup
	closeCh chan struct{}
}

func New(d driver) (*Leaser, error) {
	lsr := &Leaser{
		d: d,

		dur:   time.Minute,
		owner: genOwner(),

		leasesMux: &sync.RWMutex{},
		leases:    make(map[string]*Lease),

		wg:      &sync.WaitGroup{},
		closeCh: make(chan struct{}),
	}

	if err := lsr.tail(); err != nil {
		return nil, fmt.Errorf("tail: %w", err)
	}

	lsr.wg.Add(1)
	go func() {
		defer lsr.wg.Done()
		lsr.loopTail()
	}()

	lsr.wg.Add(1)
	go func() {
		defer lsr.wg.Done()
		lsr.loopProlong()
	}()

	return lsr, nil
}

func (lsr *Leaser) Acquire(resource string) (*Lease, error) {
	select {
	case <-lsr.closeCh:
		return nil, fmt.Errorf(`leaser is closed`)
	default:
	}

	if resource == `` {
		return nil, fmt.Errorf(`resource must be provided`)
	}

	lsr.leasesMux.RLock()
	if ls, ok := lsr.leases[resource]; ok && ls.owner != `` {
		lsr.leasesMux.RUnlock()
		return nil, ErrAlreadyAcquired
	}
	lsr.leasesMux.RUnlock()

	lsr.leasesMux.Lock()
	defer lsr.leasesMux.Unlock()

	if ls, ok := lsr.leases[resource]; ok && ls.owner != `` {
		return nil, ErrAlreadyAcquired
	}

	if err := lsr.d.Upsert(lsr.owner, resource, lsr.dur); errors.Is(err, ErrAlreadyAcquired) {
		// we know that the resource has been acquired by someone else, but we have not yet tailed it.
		lsr.leases[resource] = &Lease{
			owner:    "tbd",
			resource: resource,
		}
		return nil, err
	} else if err != nil {
		return nil, err
	}

	baseCtx, baseCtxCancel := context.WithCancel(context.Background())
	ls := &Lease{
		owner:      lsr.owner,
		resource:   resource,
		expireAt:   time.Now().Add(lsr.dur),
		canceledCh: baseCtx.Done(),
		cancel:     baseCtxCancel,
	}
	lsr.leases[resource] = ls

	return ls, nil
}

func (lsr *Leaser) Release(ls *Lease) error {
	if ls.resource == `` {
		return fmt.Errorf(`lease must be acquired; resource empty`)
	}
	if ls.owner == `` {
		return fmt.Errorf(`lease must be acquired; owner empty`)
	}

	lsr.leasesMux.RLock()
	if storedLS, ok := lsr.leases[ls.resource]; !ok {
		lsr.leasesMux.RUnlock()
		return ErrNotAcquired
	} else if storedLS.owner != ls.owner {
		lsr.leasesMux.RUnlock()
		return ErrAlreadyAcquired
	}
	lsr.leasesMux.RUnlock()

	lsr.leasesMux.Lock()
	defer lsr.leasesMux.Unlock()

	if storedLS, ok := lsr.leases[ls.resource]; ok && ls.owner != storedLS.owner {
		return ErrAlreadyAcquired
	}

	if err := lsr.d.Delete(ls.owner, ls.resource); errors.Is(err, ErrNotAcquired) {
		delete(lsr.leases, ls.resource)
		return err
	} else if err != nil {
		return fmt.Errorf("%T: %w", lsr.d, err)
	}

	delete(lsr.leases, ls.resource)

	return nil
}

func (lsr *Leaser) Close() error {
	close(lsr.closeCh)
	lsr.wg.Wait()

	return nil
}

func (lsr *Leaser) loopProlong() {

	defer lsr.leasesMux.Unlock()

	lsr.leasesMux.RLock()
	prolong := make([]*Lease, 0, len(lsr.leases))
	left := time.Now().Add(-time.Second * 20)
	for _, ls := range lsr.leases {
		if ls.expireAt.Before(left) {
			continue
		}

		prolong = append(prolong, ls)
	}
	lsr.leasesMux.RUnlock()

	if len(prolong) == 0 {
		return
	}

	lsr.leasesMux.Lock()
	defer lsr.leasesMux.Unlock()

	for _, ls := range prolong {
		if lsr.leases[ls.resource].rev != ls.rev {
			continue
		}

		if err := lsr.d.Upsert(ls.owner, ls.resource, lsr.dur); err != nil {
			log.Println("[ERROR] leaser: prolong:", err)
			ls.cancel()
		}

		ls.expireAt = time.Now().Add(lsr.dur)
		lsr.leases[ls.resource] = ls
	}
}

func (lsr *Leaser) loopTail() {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if err := lsr.tail(); err != nil {
				log.Println("[ERROR] leaser: tail:", err)
			}
		case <-lsr.closeCh:
			return
		}
	}
}

func (lsr *Leaser) tail() error {
	limit := int64(1000)
	for {
		leases, err := lsr.d.Tail(lsr.leasesSinceRev, limit)
		if err != nil {
			return fmt.Errorf("tail: %w", err)
		}

		if len(leases) == 0 {
			return nil
		}

		lsr.leasesMux.Lock()
		for _, lease := range leases {
			currLease := lsr.leases[lease.resource]
			if currLease.rev < lease.rev {
				lsr.leases[lease.resource] = lease
			}
		}
		lsr.leasesMux.Unlock()

		lsr.leasesSinceRev = leases[len(leases)-1].rev

		if int64(len(leases)) < limit {
			return nil
		}
	}
}

func genOwner() string {
	owner := uuid.New().String()
	if hostname := os.Getenv(`HOSTNAME`); hostname != `` {
		owner = fmt.Sprintf("%s/%s", hostname, owner)
	}

	return owner
}
