package co

import (
	"iter"
	"reflect"
)

type Coroutine[T any] = iter.Seq2[T, *Pending]

type Pending struct {
	Cases  []reflect.SelectCase
	Chosen int
	RecvOK bool
	Recv   reflect.Value
}

func Return[T any](f Coroutine[T], yield func(T, *Pending) bool) bool {
	for v, pending := range f {
		if !yield(v, pending) {
			return false
		}
	}
	return true
}

func Await[T any, S any](f Coroutine[T], yield func(S, *Pending) bool) (T, bool) {
	var (
		pending *Pending
		v       T
		zeroT   T
		zeroS   S
	)
	for v, pending = range f {
		if pending == nil {
			continue
		}
		if !yield(zeroS, pending) {
			return zeroT, false
		}
	}
	return v, true
}

type Poller struct {
	cases []reflect.SelectCase
}

func NewPoller(cap int) *Poller {
	return &Poller{cases: make([]reflect.SelectCase, 0, cap)}
}

func (p *Poller) Select(pendings []*Pending) int {
	p.cases = p.cases[:0]
	for _, pending := range pendings {
		p.cases = append(p.cases, pending.Cases...)
	}
	chosen, val, ok := reflect.Select(p.cases)
	for i, pending := range pendings {
		if chosen < len(pending.Cases) {
			pending.Chosen, pending.Recv, pending.RecvOK = chosen, val, ok
			return i
		}
		chosen -= len(pending.Cases)
	}
	panic("unreachable")
}

func RunOne[T any](f Coroutine[T]) {
	var pending *Pending
	for _, pending = range f {
		if pending == nil {
			continue
		}
		pending.Chosen, pending.Recv, pending.RecvOK = reflect.Select(pending.Cases)
	}
}

func EvalOne[T any](f Coroutine[T]) T {
	var v T
	var pending *Pending
	for v, pending = range f {
		if pending == nil {
			continue
		}
		pending.Chosen, pending.Recv, pending.RecvOK = reflect.Select(pending.Cases)
	}
	return v
}

func Run[T any](poller *Poller, fs []Coroutine[T]) {
	if len(fs) == 0 {
		return
	} else if len(fs) == 1 {
		RunOne(fs[0])
		return
	}
	iters := make([]struct {
		next func() (T, *Pending, bool)
		stop func()
	}, len(fs))
	for i, f := range fs {
		iters[i].next, iters[i].stop = iter.Pull2(f)
	}
	defer func() {
		for _, it := range iters {
			it.stop()
		}
	}()
	pendings := make([]*Pending, 0, len(iters))
	indexes := make([]int, 0, len(iters))
	for idx, it := range iters {
		for {
			_, pending, more := it.next()
			if !more {
				break
			}
			if pending != nil {
				pendings = append(pendings, pending)
				indexes = append(indexes, idx)
				break
			}
		}
	}
	for len(pendings) > 0 {
		k := poller.Select(pendings)
		it := iters[indexes[k]]
		for {
			_, pending, more := it.next()
			if !more {
				pendings = append(pendings[:k], pendings[k+1:]...)
				indexes = append(indexes[:k], indexes[k+1:]...)
				break
			}
			if pending != nil {
				pendings[k] = pending
				break
			}
		}
	}
}

func Eval[T any](poller *Poller, fs []Coroutine[T]) []T {
	if len(fs) == 0 {
		return nil
	} else if len(fs) == 1 {
		return []T{EvalOne(fs[0])}
	}
	vals := make([]T, len(fs))
	iters := make([]struct {
		next func() (T, *Pending, bool)
		stop func()
	}, len(fs))
	for i, f := range fs {
		iters[i].next, iters[i].stop = iter.Pull2(f)
	}
	defer func() {
		for _, it := range iters {
			it.stop()
		}
	}()
	pendings := make([]*Pending, 0, len(iters))
	indexes := make([]int, 0, len(iters))
	for idx, it := range iters {
		for {
			v, pending, more := it.next()
			if !more {
				break
			}
			if pending != nil {
				pendings = append(pendings, pending)
				indexes = append(indexes, idx)
				break
			}
			vals[idx] = v
		}
	}
	for len(pendings) > 0 {
		k := poller.Select(pendings)
		it := iters[indexes[k]]
		for {
			v, pending, more := it.next()
			if !more {
				pendings = append(pendings[:k], pendings[k+1:]...)
				indexes = append(indexes[:k], indexes[k+1:]...)
				break
			}
			if pending != nil {
				pendings[k] = pending
				break
			}
			vals[indexes[k]] = v
		}
	}
	return vals
}
