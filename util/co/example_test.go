package co

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

func ExamplePoller() {
	ch0 := make(chan int)
	ch1 := make(chan int)
	time.AfterFunc(1*time.Millisecond, func() { ch1 <- 42 })

	poller := NewPoller(1)
	pending := &Pending{
		Cases: []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch0)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch1)},
		},
	}
	selected := poller.Select([]*Pending{pending})

	fmt.Println("selected:", selected)
	fmt.Println("chosen:  ", pending.Chosen)
	fmt.Println("value:   ", pending.Recv.Interface().(int))

	// Output:
	// selected: 0
	// chosen:   1
	// value:    42
}

func ExampleRunOne() {
	ans := NewPromise[int]()
	time.AfterFunc(time.Millisecond, func() {
		ans.Fulfill(42, nil)
		fmt.Println("ans fulfilled")
	})

	f := func(yield func(Result[string], *Pending) bool) {
		fmt.Println("awaiting ans...")
		v, ok := AwaitPromise(ans, yield)
		fmt.Println("ans returned")
		if !ok {
			return
		}
		if v.Err != nil {
			yield(Result[string]{Err: v.Err}, nil)
		} else {
			yield(Result[string]{Val: fmt.Sprintf("the answer is %d", v.Val)}, nil)
		}
	}

	g := func(yield func(struct{}, *Pending) bool) {
		fmt.Println("awaiting f")
		msg, ok := Await(f, yield)
		fmt.Println("f returned")
		if !ok {
			return
		}
		if msg.Err != nil {
			return
		}
		fmt.Println(msg.Val)
	}

	RunOne(g)

	// Output:
	// awaiting f
	// awaiting ans...
	// ans fulfilled
	// ans returned
	// f returned
	// the answer is 42
}

func ExampleEvalOne() {
	ans := NewPromise[int]()
	time.AfterFunc(time.Millisecond, func() {
		ans.Fulfill(42, nil)
		fmt.Println("ans fulfilled")
	})

	f := func(yield func(Result[string], *Pending) bool) {
		fmt.Println("awaiting ans...")
		v, ok := AwaitPromise(ans, yield)
		fmt.Println("ans returned")
		if !ok {
			return
		}
		if v.Err != nil {
			yield(Result[string]{Err: v.Err}, nil)
		} else {
			yield(Result[string]{Val: fmt.Sprintf("the answer is %d", v.Val)}, nil)
		}
	}

	msg := EvalOne(f)
	if msg.Err == nil {
		fmt.Println(msg.Val)
	}

	// Output:
	// awaiting ans...
	// ans fulfilled
	// ans returned
	// the answer is 42
}

func ExampleRun() {
	a := NewPromise[int]()
	time.AfterFunc(2*time.Millisecond, func() {
		a.Fulfill(0, errors.New("oops"))
		fmt.Println("a fulfilled")
	})
	b := NewPromise[int]()
	time.AfterFunc(1*time.Millisecond, func() {
		b.Fulfill(1, nil)
		fmt.Println("b fulfilled")
	})
	pp := func(name string, promise *Promise[int]) Coroutine[struct{}] {
		return func(yield func(struct{}, *Pending) bool) {
			fmt.Println("awaiting", name, " ...")
			res, ok := AwaitPromise(promise, yield)
			if !ok {
				return
			}
			if res.Err == nil {
				fmt.Println(name, "resolved:", res.Val)
			} else {
				fmt.Println(name, "rejected:", res.Err)
			}
		}
	}

	Run(NewPoller(2), []Coroutine[struct{}]{pp("a", a), pp("b", b)})

	// Output:
	// awaiting a  ...
	// awaiting b  ...
	// b fulfilled
	// b resolved: 1
	// a fulfilled
	// a rejected: oops
}

func ExampleEval() {
	a := NewPromise[int]()
	time.AfterFunc(time.Millisecond, func() {
		a.Fulfill(1, nil)
		fmt.Println("a fulfilled")
	})
	b := NewPromise[int]()
	time.AfterFunc(2*time.Millisecond, func() {
		b.Fulfill(2, nil)
		fmt.Println("b fulfilled")
	})
	sub := func(yield func(int, *Pending) bool) {
		fmt.Println("sub awaiting b...")
		rb, ok := AwaitPromise(b, yield)
		if !ok || rb.Err != nil {
			return
		}
		fmt.Println("sub awaiting a...")
		ra, ok := AwaitPromise(a, yield)
		if !ok || ra.Err != nil {
			return
		}
		yield(rb.Val-ra.Val, nil)
	}
	add := func(yield func(int, *Pending) bool) {
		fmt.Println("add awaiting a...")
		ra, ok := AwaitPromise(a, yield)
		if !ok || ra.Err != nil {
			return
		}
		fmt.Println("add awaiting b...")
		rb, ok := AwaitPromise(b, yield)
		if !ok || rb.Err != nil {
			return
		}
		yield(rb.Val+ra.Val, nil)
	}

	ns := Eval(NewPoller(2), []Coroutine[int]{sub, add})
	fmt.Println("b - a =", ns[0])
	fmt.Println("a + b =", ns[1])

	// Output:
	// sub awaiting b...
	// add awaiting a...
	// a fulfilled
	// add awaiting b...
	// b fulfilled
	// sub awaiting a...
	// b - a = 1
	// a + b = 3
}
