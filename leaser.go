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
	leases         map[string]*lease
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
		leases:    make(map[string]*lease),

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

func (lsr *Leaser) Acquire(resource string) (Lease, error) {
	select {
	case <-lsr.closeCh:
		return Lease{}, fmt.Errorf(`leaser is closed`)
	default:
	}

	if resource == `` {
		return Lease{}, fmt.Errorf(`resource must be provided`)
	}

	ls := lsr.getOrCreate(resource)

	ls.lock()
	defer ls.unlock()

	for {
		if ls.Owner != `` {
			ls.wait()
			continue
		}

		if err := lsr.d.Upsert(lsr.owner, resource, lsr.dur); errors.Is(err, ErrAlreadyAcquired) {
			// we know that the resource has been acquired by someone else, but we have not yet tailed it.
			ls.Owner = `tbd`
			continue
		} else if err != nil {
			return Lease{}, err
		}

		ls.Owner = lsr.owner
		ls.ExpireAt = time.Now().Add(lsr.dur)
		return ls.Lease, nil
	}
}

func (lsr *Leaser) Release(ls0 Lease) error {
	if ls0.Resource == `` {
		return fmt.Errorf(`lease must be acquired; resource empty`)
	}
	if ls0.Owner == `` {
		return fmt.Errorf(`lease must be acquired; owner empty`)
	}

	ls := lsr.getOrCreate(ls0.Resource)

	ls.lock()
	defer ls.unlock()

	if ls.Owner == `` {
		return ErrNotAcquired
	} else if ls.Owner != ls0.Owner {
		return ErrAlreadyAcquired
	}

	if err := lsr.d.Delete(ls.Owner, ls.Resource); errors.Is(err, ErrNotAcquired) {
		ls.Owner = ``
		ls.Rev = 0
		ls.ExpireAt = time.Time{}
		ls.cond.Signal()

		return err
	} else if err != nil {
		return fmt.Errorf("%T: %w", lsr.d, err)
	}

	ls.Owner = ``
	ls.Rev = 0
	ls.ExpireAt = time.Time{}
	ls.cond.Signal()

	return nil
}

func (lsr *Leaser) Close() error {
	close(lsr.closeCh)
	lsr.wg.Wait()

	return nil
}

func (lsr *Leaser) getOrCreate(resource string) *lease {
	lsr.leasesMux.RLock()
	ls, ok := lsr.leases[resource]
	lsr.leasesMux.RUnlock()

	if !ok {
		lsr.leasesMux.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		lsr.leases[resource] = &lease{
			Lease: Lease{
				Owner:    ``,
				Resource: resource,
				ctx:      ctx,
			},
			cancel: cancel,
			cond:   sync.NewCond(&sync.Mutex{}),
		}
		lsr.leasesMux.Unlock()
	}

	return ls
}

func (lsr *Leaser) loopProlong() {

	lsr.leasesMux.RLock()

	prolong := make([]Lease, 0, len(lsr.leases))
	left := time.Now().Add(-time.Second * 20)
	for _, ls := range lsr.leases {
		if ls.Owner == `` {
			continue
		}
		if ls.ExpireAt.Before(left) {
			continue
		}

		prolong = append(prolong, ls.Lease)
	}
	lsr.leasesMux.RUnlock()

	if len(prolong) == 0 {
		return
	}

	for _, ls0 := range prolong {
		ls := lsr.getOrCreate(ls0.Resource)
		ls.lock()
		if ls.Rev != ls0.Rev {
			ls.unlock()
			continue
		}

		if err := lsr.d.Upsert(ls.Owner, ls.Resource, lsr.dur); err != nil {
			log.Println("[ERROR] leaser: prolong:", err)
			ls.cancel()
		}

		ls.ExpireAt = time.Now().Add(lsr.dur)
		ls.unlock()
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

		for _, tailLease := range leases {

		}

		lsr.leasesSinceRev = leases[len(leases)-1].rev

		if int64(len(leases)) < limit {
			return nil
		}
	}
}

func (lsr *Leaser) update(tailedLS Lease) {
	ls := lsr.getOrCreate(tailedLS.Resource)
	ls.lock()
	defer ls.unlock()

	if ls.Rev >= tailedLS.Rev {
		return
	}

	if ls.Owner == lsr.owner {
		ls.Lease.Rev = tailedLS.Rev
		ls.Lease.ExpireAt = time.Now().Add(lsr.dur)
	} else {
		ls.cancel()

		ctx, cancel := context.WithCancel(context.Background())
		ls.Lease = tailedLS
		ls.Lease.ctx = ctx
		ls.cancel = cancel
	}

	ls.cond.Signal()
}

func genOwner() string {
	owner := uuid.New().String()
	if hostname := os.Getenv(`HOSTNAME`); hostname != `` {
		owner = fmt.Sprintf("%s/%s", hostname, owner)
	}

	return owner
}
