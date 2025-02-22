package leaser

import (
	"context"
	"time"
)

type leaseKeyType struct{}

var leaseKey leaseKeyType

func WithLease(ctx context.Context, ls *Lease) context.Context {
	return &leaseContext{
		parent: ctx,
		ls:     ls,
	}
}

func FromLease(ctx context.Context) (*Lease, bool) {
	ls, ok := ctx.Value(leaseKey).(*Lease)
	return ls, ok
}

type leaseContext struct {
	parent context.Context
	ls     *Lease
}

func (ctx *leaseContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *leaseContext) Done() <-chan struct{} {
	return ctx.ls.сеч
}

func (ctx *leaseContext) Err() error {
	select {
	case <-ctx.ls.сеч:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx *leaseContext) Value(key any) any {
	if key == leaseKey {
		return ctx.ls
	}

	return ctx.parent.Value(key)
}
