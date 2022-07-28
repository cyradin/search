package field

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func Benchmark_SyncMonitor(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		ctx, cancel := context.WithCancel(context.Background())

		f := NewSyncMonitor(newBool()).Start(ctx)

		values := make([]uint32, cnt)
		for i := 0; i < cnt; i++ {
			values[i] = rand.Uint32()
		}
		sort.Slice(values, func(i, j int) bool {
			return values[i] < values[j]
		})

		for _, v := range values {
			f.Add(v, true)
		}

		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			wg := sync.WaitGroup{}
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					f.Term(ctx, true)
				}()
			}
			wg.Wait()
		})
		cancel()
	}
}

func Benchmark_SyncMtx(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		ctx, cancel := context.WithCancel(context.Background())

		f := NewSyncMtx(newBool())

		values := make([]uint32, cnt)
		for i := 0; i < cnt; i++ {
			values[i] = rand.Uint32()
		}
		sort.Slice(values, func(i, j int) bool {
			return values[i] < values[j]
		})

		for _, v := range values {
			f.Add(v, true)
		}

		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			wg := sync.WaitGroup{}
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					f.Term(ctx, true)
				}()
			}
			wg.Wait()
		})
		cancel()
	}
}
