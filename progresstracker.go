package merkledag

import (
	"context"
	"sync"

	cid "github.com/ipfs/go-cid"
)

// contextKey is a type to use as value for the ProgressTracker contexts.
type contextKey string

const progressContextKey contextKey = "progress"

func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		cidsToPin: make([]cid.Cid, 0),
	}
}

// WithProgressTracker returns a new context with value "progress" derived from
// the given one.
func WithProgressTracker(ctx context.Context, p *ProgressTracker) (nCtx context.Context) {
	return context.WithValue(ctx, progressContextKey, p)
}

// GetProgressTracker returns a progress tracker instance if present
func GetProgressTracker(ctx context.Context) *ProgressTracker {
	v, _ := ctx.Value(progressContextKey).(*ProgressTracker)
	return v
}

// ProgressTracker is used to show progress when fetching nodes.
type ProgressTracker struct {
	lk         sync.Mutex
	totalToPin int
	cidsToPin  []cid.Cid
}

// DeriveContext returns a new context with value "progress" derived from
// the given one.
func (p *ProgressTracker) DeriveContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, progressContextKey, p)
}

// PlanToPin registers cid as a planned to pin
func (p *ProgressTracker) PlanToPin(c cid.Cid) {
	p.lk.Lock()
	defer p.lk.Unlock()

	p.cidsToPin = append(p.cidsToPin, c)
	p.totalToPin++
}

// TotalToPin returns how much pins were planned to pin
func (p *ProgressTracker) TotalToPin() int {
	p.lk.Lock()
	defer p.lk.Unlock()

	return p.totalToPin
}

// PopPlannedToPin returns cids that were planned to pin since last call
func (p *ProgressTracker) PopPlannedToPin() []cid.Cid {
	p.lk.Lock()
	defer p.lk.Unlock()

	cids := make([]cid.Cid, len(p.cidsToPin))
	copy(cids, p.cidsToPin)
	p.cidsToPin = p.cidsToPin[:0]

	return cids
}
