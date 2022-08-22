package index

import (
	"context"
	"sync"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/storage"
	"github.com/google/uuid"
)

func newGUID() string {
	return uuid.NewString()
}

type IDs struct {
	guids map[string]uint32
	ids   map[uint32]string

	idsKey  string
	nextKey string

	mtx sync.RWMutex
}

func NewIDs(ctx context.Context, indexName string) (*IDs, error) {
	storagePrefix := storage.PrefixIndexIDs(indexName)

	idsKey := storage.MakeKey(storagePrefix, "data")
	nextKey := storage.MakeKey(storagePrefix, "next")

	vals, err := storage.DictAll[uint32](ctx, idsKey)
	if err != nil {
		return nil, errs.Errorf("ids init err: %w", err)
	}

	guids := make(map[string]uint32, len(vals))
	ids := make(map[uint32]string, len(vals))

	for guid, id := range vals {
		guids[guid] = id
		ids[id] = guid
	}

	return &IDs{
		idsKey:  idsKey,
		nextKey: nextKey,
		guids:   guids,
		ids:     ids,
	}, nil
}

func (i *IDs) NextID(ctx context.Context, guid string) (uint32, error) {
	if guid == "" {
		return 0, errs.Errorf("uid cannot be empty")
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	if id, ok := i.guids[guid]; ok {
		return id, nil
	}

	nextID64, err := storage.Increment(ctx, i.nextKey)
	if err != nil {
		return 0, err
	}
	nextID := uint32(nextID64)

	if err := storage.DictSet[uint32](ctx, i.idsKey, guid, nextID); err != nil {
		return 0, err
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

func (i *IDs) Delete(ctx context.Context, guid string) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	id := i.guids[guid]
	if id == 0 {
		return nil
	}

	if err := storage.DictDel(ctx, i.idsKey, guid); err != nil {
		return err
	}

	delete(i.guids, guid)
	delete(i.ids, id)

	return nil
}
