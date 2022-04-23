package finisher

import (
	"context"
	"sync"

	"github.com/cyradin/search/pkg/ctxt"
	"go.uber.org/zap"
)

var mtx sync.Mutex
var items []Stoppable

type Stoppable interface {
	Stop(ctx context.Context) error
}

func Add(item Stoppable) {
	mtx.Lock()
	defer mtx.Unlock()
	items = append(items, item)
}

func Wait(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			stopAll(ctx)
			return
		}
	}
}

func stopAll(ctx context.Context) {
	mtx.Lock()
	defer mtx.Unlock()
	wg := sync.WaitGroup{}
	for _, item := range items {
		wg.Add(1)
		go func(ctx context.Context, item Stoppable) {
			defer wg.Done()
			// @todo pass new, not canceled context?
			err := item.Stop(ctx)
			if err != nil {
				ctxt.Logger(ctx).Error("finisher.error", ctxt.ExtractFields(ctx, zap.Error(err))...)
			}
		}(ctx, item)
	}
	wg.Wait()
}
