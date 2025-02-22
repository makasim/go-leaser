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
	Tail(sinceRev, limit int64) ([]Lease, error)
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
	if resource == `` {
		return Lease{}, fmt.Errorf(`resource must be provided`)
	}

	select {
	case <-lsr.closeCh:
		return Lease{}, fmt.Errorf(`leaser is closed`)
	default:
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
			return Lease{}, fmt.Errorf("%T: upsert: %w", lsr.d, err)
		}

		ls.Owner = lsr.owner
		ls.Rev = 0
		ls.prolongAttempts = 0
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

	select {
	case <-lsr.closeCh:
		return fmt.Errorf(`leaser is closed`)
	default:
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
		ls.free()

		return err
	} else if err != nil {
		return fmt.Errorf("%T: %w", lsr.d, err)
	}

	ls.free()

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
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			lsr.prolong()
		case <-lsr.closeCh:
			return
		}
	}
}

func (lsr *Leaser) prolong() {
	lsr.leasesMux.RLock()

	prolong := make([]string, 0, len(lsr.leases))
	left := time.Now().Add(-time.Second * 20)
	for _, ls := range lsr.leases {
		ls.lock()
		if ls.Owner == `` {
			ls.unlock()
			continue
		}
		if ls.ExpireAt.Before(left) {
			ls.unlock()
			continue
		}

		prolong = append(prolong, ls.Resource)
		ls.unlock()
	}
	lsr.leasesMux.RUnlock()

	if len(prolong) == 0 {
		return
	}

	for _, resource := range prolong {
		lsr.prolongResource(resource, left)
	}
}

func (lsr *Leaser) prolongResource(resource string, left time.Time) {
	ls := lsr.getOrCreate(resource)
	ls.lock()
	defer ls.unlock()

	if ls.Owner == `` {
		return
	}
	if ls.ExpireAt.Before(left) {
		return
	}

	if err := lsr.d.Upsert(ls.Owner, ls.Resource, lsr.dur); errors.Is(err, ErrAlreadyAcquired) {
		log.Printf("[ERROR] leaser: prolong: %s", err)
		ls.cancel()
		ls.Owner = `tbd`
		return
	} else if err != nil {
		ls.prolongAttempts++
		if ls.prolongAttempts > 3 {
			log.Printf("[ERROR] leaser: prolong: %s", err)
			ls.cancel()
		} else {
			log.Printf("[WARN] leaser: prolong: %s", err)
		}

		return
	}

	ls.prolongAttempts = 0
	ls.ExpireAt = time.Now().Add(lsr.dur)
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
		tailedLeases, err := lsr.d.Tail(lsr.leasesSinceRev, limit)
		if err != nil {
			return fmt.Errorf("tail: %w", err)
		}

		if len(tailedLeases) == 0 {
			return nil
		}

		for _, tailedLS := range tailedLeases {
			lsr.update(tailedLS)
		}

		lsr.leasesSinceRev = tailedLeases[len(tailedLeases)-1].Rev

		if int64(len(tailedLeases)) < limit {
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
