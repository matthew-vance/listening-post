// Package pause holds a Gate that pauses and resumes long-running work in-process. The admin HTTP server toggles
// it; a loop checks Paused and watches Changed for the next transition. The backfill uses one gate for ingest and
// the archiver, so it can stop both without splitting the process.
package pause

import "sync"

// Gate is a pause/resume switch. Pause sets it, Resume clears it, and each real transition closes the channel
// Changed hands out. Both are idempotent, so a repeated Pause or Resume is a no-op.
type Gate struct {
	mu      sync.Mutex
	paused  bool
	changed chan struct{} // closed on every transition, then replaced
}

func New() *Gate {
	return &Gate{changed: make(chan struct{})}
}

func (g *Gate) Pause()  { g.set(true) }
func (g *Gate) Resume() { g.set(false) }

func (g *Gate) set(paused bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused != paused {
		g.paused = paused
		close(g.changed)
		g.changed = make(chan struct{})
	}
}

// Paused reports whether the gate is paused.
func (g *Gate) Paused() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused
}

// Changed returns a channel closed on the next Pause or Resume. Fetch it before checking Paused so a transition
// between the two is not missed.
func (g *Gate) Changed() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.changed
}
