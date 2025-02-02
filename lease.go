package leaser

import (
	"context"
	"time"
)

type Lease struct {
	owner    string
	resource string

	rev        int64
	expireAt   time.Time
	canceledCh chan struct{}
	cancel     context.CancelFunc
}

func (l *Lease) Resource() string {
	return l.resource
}

func (l *Lease) Owner() string {
	return l.owner
}
