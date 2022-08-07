package field

import (
	"fmt"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Simple interface {
	NumericConstraint | string | bool
}

type docValues[T Simple] struct {
	mtx      sync.RWMutex
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
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	return len(v.List) == 0
}

func (v *docValues[T]) Cardinality() int {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	return len(v.List)
}

func (v *docValues[T]) ContainsDoc(id uint32) bool {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	vals, ok := v.Values[id]
	return ok && len(vals) > 0
}

func (v *docValues[T]) ContainsDocValue(id uint32, value T) bool {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	vals, ok := v.Values[id]
	if !ok || len(vals) == 0 {
		return false
	}
	_, ok = vals[value]
	return ok
}

func (v *docValues[T]) ValuesByDoc(id uint32) []T {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	result := make([]T, 0, len(v.Values[id]))
	for v := range v.Values[id] {
		result = append(result, v)
	}
	return result
}

func (v *docValues[T]) DocsByIndex(i int) *roaring.Bitmap {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

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
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	vv, ok := v.Docs[value]
	if !ok {
		return roaring.New()
	}
	return vv.Clone()
}

func (v *docValues[T]) Add(id uint32, value T) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

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
	v.mtx.Lock()
	defer v.mtx.Unlock()

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

func (v *docValues[T]) listAdd(value T) {
	var index int
	switch x := any(value).(type) {
	case bool:
		if len(v.List) == 0 {
			v.List = append(v.List, value)
		} else if len(v.List) == 1 {
			if value == v.List[0] {
				return
			}
			if any(v.List[0]).(bool) == true {
				v.List = append(v.List, v.List[0])
				v.List[0] = value
			} else {
				v.List = append(v.List, value)
			}
		}
		return
	case string:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(string) })
	case int8:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int8) })
	case int16:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int16) })
	case int32:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int32) })
	case int64:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int64) })
	case uint64:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(uint64) })
	case float32:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(float32) })
	case float64:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(float64) })
	default:
		panic(fmt.Sprintf("unknown type %T", x))
	}

	if index == len(v.List) {
		v.List = append(v.List, value)
	} else if v.List[index] != value {
		v.List = append(v.List[:index+1], v.List[index:]...)
		v.List[index] = value
	}
}

func (v *docValues[T]) listDel(value T) {
	var index int
	switch x := any(value).(type) {
	case bool:
		if len(v.List) == 0 {
			return
		}

		if len(v.List) == 1 {
			if value == v.List[0] {
				v.List = nil
				return
			}
			return
		}

		if x == any(v.List[0]).(bool) {
			v.List = []T{v.List[1]}
		} else {
			v.List = []T{v.List[0]}
		}
		return
	case string:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(string) })
	case int8:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int8) })
	case int16:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int16) })
	case int32:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int32) })
	case int64:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(int64) })
	case uint64:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(uint64) })
	case float32:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(float32) })
	case float64:
		index = sort.Search(len(v.List), func(i int) bool { return x <= any(v.List[i]).(float64) })
	default:
		panic(fmt.Sprintf("unknown type %T", x))
	}

	if index == len(v.List) {
		return
	}

	if v.List[index] == value {
		v.List = append(v.List[:index], v.List[index+1:]...)
	}
}

func (f *docValues[T]) FindGt(v T) int {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	switch x := any(v).(type) {
	case bool:
		return len(f.List)
	case string:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(string) > x })
	case int8:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int8) > x })
	case int16:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int16) > x })
	case int32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int32) > x })
	case int64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int64) > x })
	case uint64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(uint64) > x })
	case float32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float32) > x })
	case float64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float64) > x })
	default:
		panic(fmt.Sprintf("unknown type %T", x))
	}
}

func (f *docValues[T]) FindGte(v T) int {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	switch x := any(v).(type) {
	case bool:
		return len(f.List)
	case string:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(string) >= x })
	case int8:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int8) >= x })
	case int16:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int16) >= x })
	case int32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int32) >= x })
	case int64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int64) >= x })
	case uint64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(uint64) >= x })
	case float32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float32) >= x })
	case float64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float64) >= x })
	default:
		panic(fmt.Sprintf("unknown type %T", x))
	}
}

func (f *docValues[T]) FindLt(v T) int {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	switch x := any(v).(type) {
	case bool:
		return len(f.List)
	case string:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(string) >= x }) - 1
	case int8:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int8) >= x }) - 1
	case int16:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int16) >= x }) - 1
	case int32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int32) >= x }) - 1
	case int64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int64) >= x }) - 1
	case uint64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(uint64) >= x }) - 1
	case float32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float32) >= x }) - 1
	case float64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float64) >= x }) - 1
	default:
		panic(fmt.Sprintf("unknown type %T", x))
	}
}

func (f *docValues[T]) FindLte(v T) int {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	switch x := any(v).(type) {
	case bool:
		return len(f.List)
	case string:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(string) > x }) - 1
	case int8:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int8) > x }) - 1
	case int16:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int16) > x }) - 1
	case int32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int32) > x }) - 1
	case int64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(int64) > x }) - 1
	case uint64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(uint64) > x }) - 1
	case float32:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float32) > x }) - 1
	case float64:
		return sort.Search(len(f.List), func(i int) bool { return any(f.List[i]).(float64) > x }) - 1
	default:
		panic(fmt.Sprintf("unknown type %T", x))
	}
}
