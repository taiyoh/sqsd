package sqsd

import (
	"time"
)

// BackOff provides exponential backoff using elapsedtime
type BackOff struct {
	dur   time.Duration
	endAt time.Time
}

// NewBackOff returns BackOff struct
func NewBackOff(elapsedSec int64) *BackOff {
	return &BackOff{
		dur: time.Duration(elapsedSec) * time.Second,
	}
}

// Continue returns whether loop should continue or not with backoff sleep.
func (b *BackOff) Continue() bool {
	now := time.Now()
	if b.endAt.IsZero() {
		b.endAt = now.Add(b.dur)
		return true
	}
	if now.Before(b.endAt) {
		time.Sleep(time.Second)
		return true
	}
	return false
}
