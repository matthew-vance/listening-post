// Package pause holds a Gate that pauses and resumes a long-running loop in-process. The admin HTTP server
// toggles it; the loop checks Paused each iteration and, once paused, blocks on WaitResume until Resume. The
// backfill uses one for ingest and one for the archiver, so it can stop either without splitting the process.
package pause

import "sync"

// Gate is a pause/resume switch. Pause sets it; Resume clears it and unblocks every waiter. Both are idempotent,
// so a repeated Pause or Resume is a no-op.
type Gate struct {
	mu      sync.Mutex
	paused  bool
	resumed chan struct{} // closed on Resume, replaced on the next Pause
}

func New() *Gate {
	return &Gate{resumed: make(chan struct{})}
}

// Pause marks the gate paused. The first Pause after a Resume replaces the resume channel so a later Resume
// unblocks only waiters that arrived since.
func (g *Gate) Pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.paused {
		g.paused = true
		g.resumed = make(chan struct{})
	}
}

// Resume marks the gate unpaused and closes the resume channel to release waiters.
func (g *Gate) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		g.paused = false
		close(g.resumed)
	}
}

// Paused reports whether the gate is paused.
func (g *Gate) Paused() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused
}

// WaitResume returns a channel closed when Resume is called. A caller should fetch it after it observes Paused,
// and re-fetch it after each Pause.
func (g *Gate) WaitResume() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.resumed
}
