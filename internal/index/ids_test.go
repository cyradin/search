package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestIDs(n int) *IDs {
	ids := NewIDs()
	for i := 0; i < n; i++ {
		guid := newGUID()
		ids.NextID(guid)
	}
	return ids
}

func Test_IDs_NextID(t *testing.T) {
	t.Run("must return err if empty guid provided", func(t *testing.T) {
		ids := NewIDs()
		id, err := ids.NextID("")
		require.Error(t, err)
		require.Empty(t, id)
	})

	t.Run("must generate next ids in sequence", func(t *testing.T) {
		ids := NewIDs()
		for i := 0; i < 5; i++ {
			guid := newGUID()
			id, err := ids.NextID(guid)
			assert.NoError(t, err)
			assert.Equal(t, uint32(i+1), id)
		}
	})

	t.Run("must not generate new ids for same guid", func(t *testing.T) {
		ids := NewIDs()
		guid := newGUID()

		id, err := ids.NextID(guid)
		require.NoError(t, err)

		id2, err := ids.NextID(guid)
		require.NoError(t, err)

		require.Equal(t, id, id2)
	})

	t.Run("must generate next ids for deleted documents", func(t *testing.T) {
		t.Run("one", func(t *testing.T) {
			ids := createTestIDs(5)

			ids.Delete(ids.UID(3))
			id, err := ids.NextID(newGUID())
			require.NoError(t, err)
			require.Equal(t, uint32(3), id)

			id, err = ids.NextID(newGUID())
			require.NoError(t, err)
			require.Equal(t, uint32(6), id)
		})

		t.Run("multiple", func(t *testing.T) {
			ids := createTestIDs(5)

			for i := 2; i <= 4; i++ {
				ids.Delete(ids.UID(uint32(i)))
			}

			for i := 2; i <= 4; i++ {
				id, err := ids.NextID(newGUID())
				require.NoError(t, err)
				require.Equal(t, uint32(i), id)
			}

			id, err := ids.NextID(newGUID())
			require.NoError(t, err)
			require.Equal(t, uint32(6), id)
		})
	})
}

func Test_IDs_UID(t *testing.T) {
	t.Run("must return empty uid if not found", func(t *testing.T) {
		ids := createTestIDs(5)
		require.Empty(t, ids.UID(100))
	})
	t.Run("must return not empty uid if found", func(t *testing.T) {
		ids := createTestIDs(5)
		guid := newGUID()
		_, err := ids.NextID(guid)
		require.NoError(t, err)
		require.Equal(t, guid, ids.UID(6))
	})
}

func Test_IDs_ID(t *testing.T) {
	t.Run("must return empty id if not found", func(t *testing.T) {
		ids := createTestIDs(5)
		require.Empty(t, ids.ID("qwerty"))
	})
	t.Run("must return not empty uid if found", func(t *testing.T) {
		ids := createTestIDs(5)
		guid := newGUID()
		id, err := ids.NextID(guid)
		require.NoError(t, err)
		require.Equal(t, id, ids.ID(guid))
	})
}

func Test_IDs_Delete(t *testing.T) {
	t.Run("must do nothing if guid not found", func(t *testing.T) {
		ids := createTestIDs(5)
		ids.Delete("qwerty")
		require.Len(t, ids.guids, 5)
	})
	t.Run("must delete guid if found", func(t *testing.T) {
		ids := createTestIDs(5)
		ids.Delete("qwerty")
		require.Len(t, ids.guids, 5)
	})
	t.Run("must return not empty uid if found", func(t *testing.T) {
		ids := createTestIDs(5)
		guid := newGUID()
		_, err := ids.NextID(guid)
		require.NoError(t, err)

		ids.Delete(guid)
		require.Len(t, ids.guids, 5)
	})
}

func Test_IDs_Marshal(t *testing.T) {
	ids := createTestIDs(5)
	ids.Delete(ids.UID(3))

	data, err := ids.MarshalBinary()
	require.NoError(t, err)

	ids2 := NewIDs()
	err = ids2.UnmarshalBinary(data)
	require.NoError(t, err)

	require.Equal(t, ids, ids2)
}
