package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortedSet_Add(t *testing.T) {
	assertData := func(t *testing.T, e, a []int64) {
		require.Equal(t, len(e), len(a))
		for i, v := range e {
			require.Equal(t, v, a[i])
		}
	}

	t.Run("add", func(t *testing.T) {
		expects := []int64{
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 5, 15, 2, 0, 0).Unix(),
			datetime(2023, 6, 10, 3, 0, 0).Unix(),
			datetime(2023, 7, 5, 4, 0, 0).Unix(),
			datetime(2023, 8, 1, 5, 0, 0).Unix(),
		}
		inputs := []int64{
			expects[3],
			expects[1],
			expects[4],
			expects[2],
			expects[0],
		}
		ss := NewSortedList()
		for _, in := range inputs {
			ss.Add(in)
		}
		assertData(t, expects, ss.Data)
	})

	t.Run("concurrent add", func(t *testing.T) {
		expects := []int64{
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 5, 15, 2, 0, 0).Unix(),
			datetime(2023, 6, 10, 3, 0, 0).Unix(),
			datetime(2023, 7, 5, 4, 0, 0).Unix(),
			datetime(2023, 8, 1, 5, 0, 0).Unix(),
		}
		inputs := []int64{
			expects[3],
			expects[1],
			expects[4],
			expects[2],
			expects[0],
		}

		forEach(100, func(_ int) {
			ss := NewSortedList()
			parallelTest(t, inputs, func(v int64) error {
				ss.Add(v)
				return nil
			}, "concurrent sorted list add test")
			assertData(t, expects, ss.Data)
		})
	})
}

func TestSortedSet_RemoveBelow(t *testing.T) {

	setupSortedSet := func(inputs []int64) *SortedList {
		ss := NewSortedList()
		for _, in := range inputs {
			ss.Add(in)
		}
		return ss
	}

	t.Run("remove", func(t *testing.T) {
		inputs := []int64{
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 5, 15, 2, 0, 0).Unix(),
			datetime(2023, 6, 10, 3, 0, 0).Unix(),
			datetime(2023, 7, 5, 4, 0, 0).Unix(),
			datetime(2023, 8, 1, 5, 0, 0).Unix(),
		}
		expects := inputs[2:]
		ss := setupSortedSet(inputs)
		threshold := datetime(2023, 6, 1, 0, 0, 0).Unix()

		ss.RemoveBelow(threshold)

		assertData(t, expects, ss.Data)
	})

	t.Run("remove, boundary remove", func(t *testing.T) {
		inputs := []int64{
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 5, 15, 2, 0, 0).Unix(),
			datetime(2023, 6, 10, 3, 0, 0).Unix(),
			datetime(2023, 7, 5, 4, 0, 0).Unix(),
			datetime(2023, 8, 1, 5, 0, 0).Unix(),
		}
		expects := inputs[2:]
		ss := setupSortedSet(inputs)
		threshold := inputs[2] // same value as already inserted factor

		ss.RemoveBelow(threshold)

		assertData(t, expects, ss.Data)
	})

	t.Run("remove, all same value", func(t *testing.T) {
		inputs := []int64{
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
		}
		expects := inputs[:]
		ss := setupSortedSet(inputs)
		threshold := inputs[0] // same value

		ss.RemoveBelow(threshold) // not remove

		assertData(t, expects, ss.Data)
	})

	t.Run("concurrent remove", func(t *testing.T) {
		inputs := []int64{
			datetime(2023, 3, 20, 1, 0, 0).Unix(),
			datetime(2023, 5, 15, 2, 0, 0).Unix(),
			datetime(2023, 6, 10, 3, 0, 0).Unix(),
			datetime(2023, 7, 5, 4, 0, 0).Unix(),
			datetime(2023, 8, 1, 5, 0, 0).Unix(),
		}

		expects := inputs[2:]

		forEach(100, func(_ int) {
			ss := setupSortedSet(inputs)
			ss.RemoveBelow(inputs[2])

			assertData(t, expects, ss.Data)
		})
	})
}

func assertData(t *testing.T, e, a []int64) {
	require.Equal(t, len(e), len(a))
	for i, v := range e {
		require.Equal(t, v, a[i])
	}
}
