package pause

import (
	"testing"
	"time"
)

func TestGate(t *testing.T) {
	g := New()
	if g.Paused() {
		t.Fatal("new gate must not be paused")
	}

	g.Pause()
	if !g.Paused() {
		t.Fatal("Pause must set paused")
	}
	g.Pause() // idempotent
	if !g.Paused() {
		t.Fatal("second Pause must be a no-op")
	}

	resumed := g.WaitResume()
	select {
	case <-resumed:
		t.Fatal("resume channel must not be closed before Resume")
	default:
	}

	g.Resume()
	if g.Paused() {
		t.Fatal("Resume must clear paused")
	}
	select {
	case <-resumed:
	default:
		t.Fatal("Resume must close the resume channel")
	}
	g.Resume() // idempotent: must not panic on a second close
}

func TestWaitResumeUnblocksBeforeWait(t *testing.T) {
	g := New()
	g.Pause()
	done := make(chan struct{})
	go func() {
		<-g.WaitResume()
		close(done)
	}()
	g.Resume()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Resume must unblock a waiter")
	}
}

func TestRePauseReplacesChannel(t *testing.T) {
	g := New()
	g.Pause()
	first := g.WaitResume()
	g.Resume()
	g.Pause()
	second := g.WaitResume()
	if first == second {
		t.Fatal("a fresh Pause must hand out a new resume channel")
	}
	select {
	case <-second:
		t.Fatal("a re-paused gate's channel must be open")
	default:
	}
}
