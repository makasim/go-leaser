package leaser

import (
	"context"
	"sync"
	"time"
)

type Lease struct {
	Owner    string
	Resource string
	Rev      int64
	ExpireAt time.Time

	ctx context.Context
}

func (l *Lease) Done() <-chan struct{} {
	return l.ctx.Done()
}

type lease struct {
	Lease

	cancel context.CancelFunc
	cond   *sync.Cond
}

func (l *lease) lock() {
	l.cond.L.Lock()
}

func (l *lease) unlock() {
	l.cond.L.Unlock()
}

func (l *lease) wait() {
	l.cond.Wait()
}
