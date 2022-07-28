package field

import (
	"context"
	"sync"

	"github.com/cyradin/search/internal/index/schema"
)

type syncAdd struct {
	id    uint32
	value interface{}
	ch    chan struct{}
}

type syncTerm struct {
	ctx   context.Context
	value interface{}
	ch    chan *Result
}

type syncMatch struct {
	ctx   context.Context
	value interface{}
	ch    chan *Result
}

type syncRange struct {
	ctx     context.Context
	from    interface{}
	to      interface{}
	incFrom bool
	incTo   bool
	ch      chan *Result
}

type syncDelete struct {
	id uint32
	ch chan struct{}
}

type syncData struct {
	id uint32
	ch chan []interface{}
}

type syncMarshalResult struct {
	data []byte
	err  error
}

type syncMarshal struct {
	ch chan syncMarshalResult
}

type syncUnmarshalResult struct {
	err error
}

type syncUnmarshal struct {
	data []byte
	ch   chan syncUnmarshalResult
}

var _ Field = (*SyncMonitor)(nil)

// SyncMonitor field-wrapper used to ensure multi-thread safety.
// This implementation is currently slower than SyncMtx, so it is not used for now.
type SyncMonitor struct {
	field   Field
	started bool

	chAdd       chan syncAdd
	chTerm      chan syncTerm
	chMatch     chan syncMatch
	chRange     chan syncRange
	chDelete    chan syncDelete
	chData      chan syncData
	chMarshal   chan syncMarshal
	chUnmarshal chan syncUnmarshal
}

func NewSyncMonitor(field Field) *SyncMonitor {
	return &SyncMonitor{
		field:       field,
		chAdd:       make(chan syncAdd),
		chTerm:      make(chan syncTerm),
		chMatch:     make(chan syncMatch),
		chRange:     make(chan syncRange),
		chDelete:    make(chan syncDelete),
		chData:      make(chan syncData),
		chMarshal:   make(chan syncMarshal),
		chUnmarshal: make(chan syncUnmarshal),
	}
}

func (f *SyncMonitor) Start(ctx context.Context) *SyncMonitor {
	if f.started {
		panic("monitor already started")
	}
	f.monitor(ctx)
	return f
}

func (f *SyncMonitor) Type() schema.Type {
	return f.field.Type()
}

func (f *SyncMonitor) Add(id uint32, value interface{}) {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan struct{})
	v := syncAdd{id, value, ch}
	f.chAdd <- v
	<-ch
}

func (f *SyncMonitor) Term(ctx context.Context, value interface{}) *Result {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan *Result)
	v := syncTerm{ctx, value, ch}
	f.chTerm <- v
	return <-ch
}

func (f *SyncMonitor) Match(ctx context.Context, value interface{}) *Result {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan *Result)
	v := syncMatch{ctx, value, ch}
	f.chMatch <- v
	return <-ch
}

func (f *SyncMonitor) Range(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *Result {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan *Result)
	v := syncRange{ctx, from, to, incFrom, incTo, ch}
	f.chRange <- v
	return <-ch
}

func (f *SyncMonitor) Delete(id uint32) {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan struct{})
	v := syncDelete{id, ch}
	f.chDelete <- v
	<-ch
}

func (f *SyncMonitor) Data(id uint32) []interface{} {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan []interface{})
	v := syncData{id, ch}
	f.chData <- v
	return <-ch
}

func (f *SyncMonitor) MarshalBinary() ([]byte, error) {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan syncMarshalResult)
	v := syncMarshal{ch}
	f.chMarshal <- v
	result := <-ch
	return result.data, result.err
}

func (f *SyncMonitor) UnmarshalBinary(data []byte) error {
	if !f.started {
		panic("monitor not started")
	}
	ch := make(chan syncUnmarshalResult)
	v := syncUnmarshal{data, ch}
	f.chUnmarshal <- v
	result := <-ch
	return result.err
}

func (f *SyncMonitor) monitor(ctx context.Context) {
	f.started = true

	go func() {
		wg := sync.WaitGroup{}
		for {
			select {
			// async actions
			case v := <-f.chTerm:
				wg.Add(1)
				go func() {
					defer wg.Done()
					v.ch <- f.field.Term(v.ctx, v.value)
				}()
			case v := <-f.chMatch:
				wg.Add(1)
				go func() {
					defer wg.Done()
					v.ch <- f.field.Match(v.ctx, v.value)
				}()
			case v := <-f.chRange:
				wg.Add(1)
				go func() {
					defer wg.Done()
					v.ch <- f.field.Range(v.ctx, v.from, v.to, v.incFrom, v.incTo)
				}()
			case v := <-f.chData:
				wg.Add(1)
				go func() {
					defer wg.Done()
					v.ch <- f.field.Data(v.id)
				}()

			// sync actions
			case v := <-f.chAdd:
				wg.Wait()
				f.field.Add(v.id, v.value)
				v.ch <- struct{}{}
			case v := <-f.chDelete:
				wg.Wait()
				f.field.Delete(v.id)
				v.ch <- struct{}{}
			case v := <-f.chMarshal:
				wg.Wait()
				r, err := f.field.MarshalBinary()
				v.ch <- syncMarshalResult{r, err}
			case v := <-f.chUnmarshal:
				wg.Wait()
				err := f.field.UnmarshalBinary(v.data)
				v.ch <- syncUnmarshalResult{err}
			case <-ctx.Done():
				wg.Wait()
				return
			}
		}
	}()
}

// SyncMtx field-wrapper used to ensure multi-thread safety.
// Uses mutex under the hood
type SyncMtx struct {
	mtx   sync.RWMutex
	field Field
}

func NewSyncMtx(field Field) *SyncMtx {
	return &SyncMtx{field: field}
}

func (f *SyncMtx) Type() schema.Type {
	return f.field.Type()
}

func (f *SyncMtx) Add(id uint32, value interface{}) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field.Add(id, value)
}

func (f *SyncMtx) Term(ctx context.Context, value interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Term(ctx, value)
}

func (f *SyncMtx) Match(ctx context.Context, value interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Match(ctx, value)
}

func (f *SyncMtx) Range(ctx context.Context, from interface{}, to interface{}, incFrom, incTo bool) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Range(ctx, from, to, incFrom, incTo)
}

func (f *SyncMtx) Delete(id uint32) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field.Delete(id)
}

func (f *SyncMtx) Data(id uint32) []interface{} {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.field.Data(id)
}

func (f *SyncMtx) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.field.MarshalBinary()
}

func (f *SyncMtx) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.field.UnmarshalBinary(data)
}
