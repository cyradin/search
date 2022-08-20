package storage

import "testing"

func Benchmark_makeKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		makeKey("prefix", "key")
	}
}
