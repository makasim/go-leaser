package leaser

import "fmt"

var (
	ErrNotAcquired     = fmt.Errorf("lease not acquired")
	ErrAlreadyAcquired = fmt.Errorf("lease already acquired")
)
