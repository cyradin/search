package events

import (
	"context"
	"sync"
)

var handlersMtx sync.RWMutex
var handlersStore = map[string][]Handler{}

type Handler func(ctx context.Context, e Event)

type Event interface {
	Code() string
}

func Dispatch(ctx context.Context, e Event) {
	go func(ctx context.Context) {
		handlersMtx.RLock()
		defer handlersMtx.RUnlock()
		if handlers, ok := handlersStore[e.Code()]; ok {
			for _, handler := range handlers {
				handler(ctx, e)
			}
		}
	}(ctx)
}

func Subscribe(e Event, h Handler) {
	handlersMtx.Lock()
	defer handlersMtx.Unlock()

	code := e.Code()
	handlersStore[code] = append(handlersStore[code], h)
}

func Unsubscribe(e Event) {
	handlersMtx.Lock()
	defer handlersMtx.Unlock()

	delete(handlersStore, e.Code())
}
