package internal

import (
	"sort"
	"sync"
)

type SortedList struct {
	Data []int64
	mu   sync.RWMutex
}

func NewSortedList() *SortedList {
	return &SortedList{Data: make([]int64, 0)}
}

func (s *SortedList) Add(value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Data = append(s.Data, value)
	sort.Slice(s.Data, func(i, j int) bool {
		return s.Data[i] < s.Data[j]
	})
}

func (s *SortedList) RemoveBelow(threshold int64) {
	s.mu.RLock()
	index := sort.Search(len(s.Data), func(i int) bool {
		return s.Data[i] >= threshold // same value is not removed
	})
	s.mu.RUnlock()
	if index > 0 {
		s.mu.Lock()
		s.Data = s.Data[index:]
		s.mu.Unlock()
	}
}
