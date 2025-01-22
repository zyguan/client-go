package co

import (
	"reflect"
	"sync"
)

type Result[T any] struct {
	Val T
	Err error
}

type Promise[T any] struct {
	once sync.Once
	done chan struct{}
	res  Result[T]
}

func NewPromise[T any]() *Promise[T] { return &Promise[T]{done: make(chan struct{})} }

func (p *Promise[T]) Pending() *Pending {
	select {
	case <-p.done:
		return nil
	default:
		return &Pending{
			Cases: []reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.done)}},
		}
	}
}

func (p *Promise[T]) Get() Result[T] {
	select {
	case <-p.done:
		return p.res
	default:
		return Result[T]{}
	}
}

func (p *Promise[T]) Fulfill(val T, err error) {
	p.once.Do(func() {
		p.res.Val, p.res.Err = val, err
		close(p.done)
	})
}

func AwaitPromise[T any, S any](p *Promise[T], yield func(S, *Pending) bool) (Result[T], bool) {
	var zeroS S
	pending := p.Pending()
	if pending == nil {
		return p.Get(), true
	}
	if !yield(zeroS, pending) {
		return Result[T]{}, false
	}
	return p.Get(), true
}
