package leaser

import (
	"context"
)

type leaseKeyType struct{}

var leaseKey leaseKeyType

func WithLease(parent context.Context, ls *Lease) context.Context {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		<-ls.Done()
		cancel()
	}()

	return context.WithValue(ctx, leaseKey, ls)
}

func FromLease(ctx context.Context) (*Lease, bool) {
	ls, ok := ctx.Value(leaseKey).(*Lease)
	return ls, ok
}
