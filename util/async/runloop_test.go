package async

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecWait(t *testing.T) {
	var list []int
	l := NewRunLoop()
	time.AfterFunc(time.Millisecond, func() {
		l.Append(func() {
			list = append(list, 1)
		})
	})
	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)
}

func TestExecOnce(t *testing.T) {
	var list []int
	l := NewRunLoop()
	l.Append(func() {
		l.Append(func() {
			list = append(list, 2)
		})
		list = append(list, 1)
	})

	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 2, n)
	require.Equal(t, []int{1, 2}, list)
}

func TestExecTwice(t *testing.T) {
	var list []int
	l := NewRunLoop()
	l.Append(func() {
		time.AfterFunc(time.Millisecond, func() {
			l.Append(func() {
				list = append(list, 2)
			})
		})
		list = append(list, 1)
	})

	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)

	n, err = l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1, 2}, list)
}

func TestExecCancelWhileRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var list []int
	l := NewRunLoop()
	l.Append(
		func() {
			cancel()
			list = append(list, 1)
		},
		func() {
			list = append(list, 2)
		},
	)

	n, err := l.Exec(ctx)
	require.Error(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 1, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)
}

func TestExecCancelWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	l := NewRunLoop()
	time.AfterFunc(time.Millisecond, cancel)

	n, err := l.Exec(ctx)
	require.Error(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 0, n)
}

func TestExecConcurrent(t *testing.T) {
	l := NewRunLoop()
	l.Append(func() {
		time.Sleep(time.Millisecond)
	})
	done := make(chan struct{})
	go func() {
		n, err := l.Exec(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, n)
		close(done)
	}()
	runtime.Gosched()
	n, err := l.Exec(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, n)
	<-done
}
