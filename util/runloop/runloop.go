package runloop

import (
	"context"
	"errors"
	"sync"
)

type State uint32

const (
	StateIdle State = iota
	StateWaiting
	StateRunning
)

type RunLoop struct {
	lock     sync.Mutex
	ready    chan struct{}
	runnable []func()
	running  []func()
	state    State
}

func New() *RunLoop {
	return &RunLoop{ready: make(chan struct{})}
}

func (l *RunLoop) State() State {
	l.lock.Lock()
	state := l.state
	l.lock.Unlock()
	return state
}

func (l *RunLoop) NumRunnable() int {
	l.lock.Lock()
	n := len(l.runnable)
	l.lock.Unlock()
	return n
}

func (l *RunLoop) Append(fs ...func()) {
	if len(fs) == 0 {
		return
	}

	notify := false

	l.lock.Lock()
	l.runnable = append(l.runnable, fs...)
	if l.state == StateWaiting {
		l.state = StateIdle // waiting -> idle
		notify = true
	}
	l.lock.Unlock()

	if notify {
		l.ready <- struct{}{}
	}
}

func (l *RunLoop) Exec(ctx context.Context) (int, error) {
	for {
		l.lock.Lock()
		if l.state != StateIdle {
			l.lock.Unlock()
			return 0, errors.New("runloop: already executing")
		}
		// assert l.state == stateIdle

		if len(l.runnable) == 0 {
			l.state = StateWaiting // idle -> waiting
			l.lock.Unlock()
			select {
			case <-l.ready:
				continue
			case <-ctx.Done():
				l.lock.Lock()
				l.state = StateIdle // waiting -> idle
				l.lock.Unlock()
				return 0, ctx.Err()
			}
		} else {
			l.running, l.runnable = l.runnable, l.running[:0]
			l.state = StateRunning // idle -> running
			l.lock.Unlock()
			return l.run(ctx)
		}
	}
}

func (l *RunLoop) run(ctx context.Context) (int, error) {
	count := 0
	for {
		for i, f := range l.running {
			select {
			case <-ctx.Done():
				l.lock.Lock()
				// move remaining running tasks to runnable
				l.running = append(l.running[:0], l.running[i:]...)
				l.running = append(l.running, l.runnable...)
				l.running, l.runnable = l.runnable, l.running
				l.state = StateIdle // running -> idle
				l.lock.Unlock()
				return count, ctx.Err()
			default:
				f()
				count++
			}
		}
		l.lock.Lock()
		if len(l.runnable) == 0 {
			l.state = StateIdle // running -> idle
			l.lock.Unlock()
			return count, nil
		}
		l.running, l.runnable = l.runnable, l.running[:0]
		l.lock.Unlock()
	}
}
