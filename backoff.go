package sqsd

import (
	"time"

	"github.com/jpillora/backoff"
)

// BackOff provides exponential backoff using elapsedtime
type BackOff struct {
	ebo     *backoff.Backoff
	isFirst bool
}

// NewBackOff returns BackOff struct
func NewBackOff(elapsedSec int64) *BackOff {
	b := &backoff.Backoff{
		Min:    time.Second,
		Max:    time.Duration(elapsedSec) * time.Second,
		Factor: float64(elapsedSec),
		Jitter: false,
	}
	return &BackOff{
		ebo:     b,
		isFirst: true,
	}
}

// Continue returns whether loop should continue or not with backoff sleep.
func (b *BackOff) Continue() bool {
	if b.isFirst {
		b.isFirst = false
		return true
	}
	if b.ebo.Attempt() < b.ebo.Factor {
		time.Sleep(b.ebo.Duration())
		return true
	}
	return false
}
