package index

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/cyradin/search/internal/errs"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
)

func newGUID() string {
	return uuid.NewString()
}

type IDs struct {
	guids map[string]uint32
	ids   map[uint32]string
	next  uint32

	free []uint32

	mtx sync.RWMutex
}

func NewIDs() *IDs {
	return &IDs{
		guids: make(map[string]uint32, 1000),
		ids:   make(map[uint32]string, 1000),
		next:  0,
		free:  make([]uint32, 0, 1000),
	}
}

func (i *IDs) NextID(guid string) (uint32, error) {
	if guid == "" {
		return 0, errs.Errorf("uid cannot be empty")
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	if id, ok := i.guids[guid]; ok {
		return id, nil
	}

	var nextID uint32
	if len(i.free) > 0 {
		nextID = i.free[0]
		i.free = slices.Delete(i.free, 0, 1)
	} else {
		i.next++
		nextID = i.next
	}

	i.guids[guid] = nextID
	i.ids[nextID] = guid

	return nextID, nil
}

func (i *IDs) ID(guid string) uint32 {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	return i.guids[guid]
}

func (i *IDs) UID(id uint32) string {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	return i.ids[id]
}

func (i *IDs) Delete(guid string) {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	id := i.guids[guid]
	if id == 0 {
		return
	}

	delete(i.guids, guid)
	delete(i.ids, id)

	i.free = append(i.free, id)
}

type idsData struct {
	Guids map[string]uint32
	Ids   map[uint32]string
	Next  uint32
	Free  []uint32
}

func (f *IDs) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(idsData{
		Guids: f.guids,
		Ids:   f.ids,
		Next:  f.next,
		Free:  f.free,
	})

	return buf.Bytes(), err
}

func (f *IDs) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	raw := idsData{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.guids = raw.Guids
	f.ids = raw.Ids
	f.free = raw.Free
	f.next = raw.Next

	return nil
}
