package async

import (
	"sync"
)

type Executor interface {
	// Append adds functions to the executor.
	Append(fs ...func())
}

type Callback[T any] interface {
	// Executor returns the executor that the callback uses.
	Executor() Executor
	// Inject adds a deferred action that will be invoked before the callback.
	Inject(g func(T, error) (T, error))
	// Invoke invokes the callback immediately in current goroutine.
	Invoke(val T, err error)
	// Schedule schedules the callback to be invoked later, it's typically called in other goroutines.
	Schedule(val T, err error)
}

func NewCallback[T any](e Executor, f func(T, error)) Callback[T] {
	return &callback[T]{e: e, f: f}
}

type callback[T any] struct {
	once sync.Once
	e    Executor
	f    func(T, error)
	gs   []func(T, error) (T, error)
}

// Executor implements Callback.
func (cb *callback[T]) Executor() Executor {
	return cb.e
}

// Inject implements Callback.
func (cb *callback[T]) Inject(g func(T, error) (T, error)) {
	cb.gs = append(cb.gs, g)
}

// Invoke implements Callback.
func (cb *callback[T]) Invoke(val T, err error) {
	cb.once.Do(func() { cb.call(val, err) })
}

// Schedule implements Callback.
func (cb *callback[T]) Schedule(val T, err error) {
	cb.once.Do(func() { cb.e.Append(func() { cb.call(val, err) }) })
}

func (cb *callback[T]) call(val T, err error) {
	for i := len(cb.gs) - 1; i >= 0; i-- {
		val, err = cb.gs[i](val, err)
	}
	cb.f(val, err)
}
