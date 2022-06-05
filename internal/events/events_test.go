package events

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type testEvent struct{}

func (e testEvent) Code() string {
	return "test"
}

func Test_EventDispatcher_Dispatch(t *testing.T) {
	e := testEvent{}
	d := NewEventDispatcher()

	wg := sync.WaitGroup{}
	wg.Add(2)
	executed := 0

	d.handlersStore[e.Code()] = []Handler{
		func(ctx context.Context, e Event) {
			wg.Done()
			executed++
		},
		func(ctx context.Context, e Event) {
			wg.Done()
			executed++
		},
	}

	d.Dispatch(context.Background(), e)
	wg.Wait()

	require.Equal(t, 2, executed)
}

func Test_EventDispatcher_Subscribe(t *testing.T) {
	e := testEvent{}
	d := NewEventDispatcher()

	wg := sync.WaitGroup{}
	wg.Add(3)
	executed := 0

	f := func(ctx context.Context, e Event) {
		wg.Done()
		executed++
	}

	d.Subscribe(e, f)
	d.Subscribe(e, f)
	d.Subscribe(e, f)

	d.Dispatch(context.Background(), e)
	wg.Wait()

	require.Equal(t, 3, executed)
}

func Test_EventDispatcher_Unsubscribe(t *testing.T) {
	e := testEvent{}
	d := NewEventDispatcher()

	f := func(ctx context.Context, e Event) {}
	d.Subscribe(e, f)
	require.Len(t, d.handlersStore, 1)
	d.Unsubscribe(e)
	require.Len(t, d.handlersStore, 0)
}
