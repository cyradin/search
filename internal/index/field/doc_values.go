package field

import (
	"sort"

	"github.com/RoaringBitmap/roaring"
)

type Simple interface {
	NumericConstraint | string
}

type docValues[T Simple] struct {
	Docs     map[T]*roaring.Bitmap
	Values   map[uint32]docValue[T]
	Counters map[T]int
	List     []T
}
type docValue[T Simple] map[T]struct{}

func newDocValues[T Simple]() *docValues[T] {
	return &docValues[T]{
		Docs:     make(map[T]*roaring.Bitmap),
		Values:   make(map[uint32]docValue[T]),
		Counters: make(map[T]int),
	}
}

func (v *docValues[T]) IsEmpty() bool {
	return len(v.List) == 0
}

func (v *docValues[T]) Cardinality() int {
	return len(v.List)
}

func (v *docValues[T]) ContainsDoc(id uint32) bool {
	vals, ok := v.Values[id]
	return ok && len(vals) > 0
}

func (v *docValues[T]) ContainsDocValue(id uint32, value T) bool {
	vals, ok := v.Values[id]
	if !ok || len(vals) == 0 {
		return false
	}
	_, ok = vals[value]
	return ok
}

func (v *docValues[T]) ValuesByDoc(id uint32) []T {
	result := make([]T, 0, len(v.Values[id]))
	for v := range v.Values[id] {
		result = append(result, v)
	}
	return result
}

func (v *docValues[T]) DocsByIndex(i int) *roaring.Bitmap {
	if i < 0 || i >= len(v.List) {
		return roaring.New()
	}

	vv, ok := v.Docs[v.List[i]]
	if !ok {
		return roaring.New()
	}
	return vv
}

func (v *docValues[T]) DocsByValue(value T) *roaring.Bitmap {
	vv, ok := v.Docs[value]
	if !ok {
		return roaring.New()
	}
	return vv
}

func (v *docValues[T]) Add(id uint32, value T) {
	if v.Values[id] == nil {
		v.Values[id] = make(map[T]struct{})
	}
	v.Values[id][value] = struct{}{}

	if v.Counters[value] == 0 {
		v.listAdd(value)
	}
	v.Counters[value]++

	if v.Docs[value] == nil {
		v.Docs[value] = roaring.New()
	}
	v.Docs[value].Add(id)
}

func (v *docValues[T]) DeleteDoc(id uint32) {
	vals, ok := v.Values[id]
	if !ok {
		return
	}
	delete(v.Values, id)

	for vv := range vals {
		v.Counters[vv]--
		if v.Counters[vv] == 0 {
			v.listDel(vv)
			delete(v.Counters, vv)
		}
		v.Docs[vv].Remove(id)
		if v.Docs[vv].GetCardinality() == 0 {
			delete(v.Docs, vv)
		}
	}
}

func (f *docValues[T]) FindGt(v T) int {
	return sort.Search(len(f.List), func(i int) bool { return f.List[i] > v })
}

func (f *docValues[T]) FindGte(v T) int {
	return sort.Search(len(f.List), func(i int) bool { return f.List[i] >= v })
}

func (f *docValues[T]) FindLt(v T) int {
	return sort.Search(len(f.List), func(i int) bool { return f.List[i] >= v }) - 1
}

func (f *docValues[T]) FindLte(v T) int {
	return sort.Search(len(f.List), func(i int) bool { return f.List[i] > v }) - 1
}

func (v *docValues[T]) listAdd(value T) {
	index := sort.Search(len(v.List), func(i int) bool { return value <= v.List[i] })
	if index == len(v.List) {
		v.List = append(v.List, value)
	} else if v.List[index] != value {
		v.List = append(v.List[:index+1], v.List[index:]...)
		v.List[index] = value
	}
}

func (v *docValues[T]) listDel(value T) {
	index := sort.Search(len(v.List), func(i int) bool { return value <= v.List[i] })
	if index == len(v.List) {
		return
	}

	if v.List[index] == value {
		v.List = append(v.List[:index], v.List[index+1:]...)
	}
}
