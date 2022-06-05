package events

import (
	"context"
	"sync"
)

type Handler func(ctx context.Context, e Event)

type Event interface {
	Code() string
}

var DefaultEventDispatcher = NewEventDispatcher()

func Dispatch(ctx context.Context, e Event) {
	DefaultEventDispatcher.Dispatch(ctx, e)
}

func Subscribe(e Event, h Handler) {
	DefaultEventDispatcher.Subscribe(e, h)
}

func Unsubscribe(e Event) {
	DefaultEventDispatcher.Unsubscribe(e)
}

type EventDispatcher struct {
	handlersMtx   sync.RWMutex
	handlersStore map[string][]Handler
}

func NewEventDispatcher() *EventDispatcher {
	return &EventDispatcher{
		handlersStore: make(map[string][]Handler),
	}
}

func (d *EventDispatcher) Dispatch(ctx context.Context, e Event) {
	go func(ctx context.Context) {
		d.handlersMtx.RLock()
		defer d.handlersMtx.RUnlock()
		if handlers, ok := d.handlersStore[e.Code()]; ok {
			for _, handler := range handlers {
				handler(ctx, e)
			}
		}
	}(ctx)
}

func (d *EventDispatcher) Subscribe(e Event, h Handler) {
	d.handlersMtx.Lock()
	defer d.handlersMtx.Unlock()

	code := e.Code()
	d.handlersStore[code] = append(d.handlersStore[code], h)
}

func (d *EventDispatcher) Unsubscribe(e Event) {
	d.handlersMtx.Lock()
	defer d.handlersMtx.Unlock()

	delete(d.handlersStore, e.Code())
}
