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

func Test_Dispatch(t *testing.T) {
	t.Cleanup(func() {
		handlersStore = map[string][]Handler{}
	})

	e := testEvent{}

	wg := sync.WaitGroup{}
	wg.Add(2)
	executed := 0

	handlersStore[e.Code()] = []Handler{
		func(ctx context.Context, e Event) {
			wg.Done()
			executed++
		},
		func(ctx context.Context, e Event) {
			wg.Done()
			executed++
		},
	}

	Dispatch(context.Background(), e)
	wg.Wait()

	require.Equal(t, 2, executed)
}

func Test_Subscribe(t *testing.T) {
	t.Cleanup(func() {
		handlersStore = map[string][]Handler{}
	})

	e := testEvent{}

	wg := sync.WaitGroup{}
	wg.Add(3)
	executed := 0

	f := func(ctx context.Context, e Event) {
		wg.Done()
		executed++
	}

	Subscribe(e, f)
	Subscribe(e, f)
	Subscribe(e, f)

	Dispatch(context.Background(), e)
	wg.Wait()

	require.Equal(t, 3, executed)
}

func Test_Unsubscribe(t *testing.T) {
	t.Cleanup(func() {
		handlersStore = map[string][]Handler{}
	})

	e := testEvent{}

	f := func(ctx context.Context, e Event) {}
	Subscribe(e, f)
	require.Len(t, handlersStore, 1)
	Unsubscribe(e)
	require.Len(t, handlersStore, 0)
}
