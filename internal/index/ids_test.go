package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestIDs(ctx context.Context, t *testing.T, n int) *IDs {
	ids, err := NewIDs(ctx, "")
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		guid := newGUID()
		ids.NextID(ctx, guid)
	}
	return ids
}

func Test_IDs_NextID(t *testing.T) {
	t.Run("must return err if empty guid provided", func(t *testing.T) {
		ctx := testContext(t)
		ids, err := NewIDs(ctx, "")
		require.NoError(t, err)

		id, err := ids.NextID(ctx, "")
		require.Error(t, err)
		require.Empty(t, id)
	})

	t.Run("must generate next ids in sequence", func(t *testing.T) {
		ctx := testContext(t)
		ids, err := NewIDs(ctx, "")
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			guid := newGUID()
			id, err := ids.NextID(ctx, guid)
			assert.NoError(t, err)
			assert.Equal(t, uint32(i+1), id)
		}
	})

	t.Run("must not generate new ids for same guid", func(t *testing.T) {
		ctx := testContext(t)
		ids, err := NewIDs(ctx, "")
		require.NoError(t, err)
		guid := newGUID()

		id, err := ids.NextID(ctx, guid)
		require.NoError(t, err)

		id2, err := ids.NextID(ctx, guid)
		require.NoError(t, err)

		require.Equal(t, id, id2)
	})
}

func Test_IDs_UID(t *testing.T) {
	t.Run("must return empty uid if not found", func(t *testing.T) {
		ctx := testContext(t)
		ids := createTestIDs(ctx, t, 5)
		require.Empty(t, ids.UID(100))
	})
	t.Run("must return not empty uid if found", func(t *testing.T) {
		ctx := testContext(t)
		ids := createTestIDs(ctx, t, 5)
		guid := newGUID()
		_, err := ids.NextID(ctx, guid)
		require.NoError(t, err)
		require.Equal(t, guid, ids.UID(6))
	})
}

func Test_IDs_ID(t *testing.T) {
	t.Run("must return empty id if not found", func(t *testing.T) {
		ctx := testContext(t)
		ids := createTestIDs(ctx, t, 5)
		require.Empty(t, ids.ID("qwerty"))
	})
	t.Run("must return not empty uid if found", func(t *testing.T) {
		ctx := testContext(t)
		ids := createTestIDs(ctx, t, 5)
		guid := newGUID()
		id, err := ids.NextID(ctx, guid)
		require.NoError(t, err)
		require.Equal(t, id, ids.ID(guid))
	})
}

func Test_IDs_Delete(t *testing.T) {
	t.Run("must do nothing if guid not found", func(t *testing.T) {
		ctx := testContext(t)
		ids := createTestIDs(ctx, t, 5)
		err := ids.Delete(ctx, "qwerty")
		require.Len(t, ids.guids, 5)
		require.NoError(t, err)
	})
	t.Run("must delete guid if found", func(t *testing.T) {
		ctx := testContext(t)
		ids := createTestIDs(ctx, t, 5)
		err := ids.Delete(ctx, ids.ids[2])
		require.Len(t, ids.guids, 4)
		require.Len(t, ids.ids, 4)
		require.NoError(t, err)
	})
}
