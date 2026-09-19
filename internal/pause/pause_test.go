package pause

import "testing"

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestGate(t *testing.T) {
	g := New()
	if g.Paused() {
		t.Fatal("new gate must not be paused")
	}

	ch := g.Changed()
	g.Pause()
	if !g.Paused() || !closed(ch) {
		t.Fatal("Pause must set paused and close the pending Changed channel")
	}

	ch = g.Changed()
	g.Pause() // idempotent: no transition, so no close
	if closed(ch) {
		t.Fatal("a repeated Pause must not close Changed")
	}

	g.Resume()
	if g.Paused() || !closed(ch) {
		t.Fatal("Resume must clear paused and close the pending Changed channel")
	}

	ch = g.Changed()
	g.Resume() // idempotent
	if closed(ch) {
		t.Fatal("a repeated Resume must not close Changed")
	}
}
