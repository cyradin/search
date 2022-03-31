package index

import "sync"

type docIDs map[string]uint32

type IDs struct {
	mtx  sync.Mutex
	max  uint32
	docs docIDs
}

func NewIDs(max uint32, docs docIDs) *IDs {
	if docs == nil {
		docs = make(docIDs)
	}

	return &IDs{
		max:  max,
		docs: docs,
	}
}

func (i *IDs) Get(uid string) uint32 {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	id := i.docs[uid]

	return id
}

func (i *IDs) Set(uid string) uint32 {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	if id, ok := i.docs[uid]; ok {
		return id
	}

	id := i.next()
	i.docs[uid] = id

	return id
}

func (i *IDs) current() uint32 {
	return i.max
}

func (i *IDs) next() uint32 {
	i.max++
	return i.max
}
