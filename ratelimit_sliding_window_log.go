package ratelimiter

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"rate-limiter/internal"
)

type SlidingWindowLogRateLimiter struct {
	limit    int
	duration time.Duration

	// manage sorted timestamp table
	// in fact, should use SortedSet in redis
	sortedTimestamps *internal.SortedList

	mu sync.RWMutex
}

func NewSlidingWindowLogRateLimiter(limit int, duration time.Duration) *SlidingWindowLogRateLimiter {
	tokenBucket := SlidingWindowLogRateLimiter{
		limit:            limit,
		sortedTimestamps: internal.NewSortedList(),
		duration:         duration,
	}

	return &tokenBucket
}

func (b *SlidingWindowLogRateLimiter) UsageCount() int {
	return slices.Max([]int{b.limit - len(b.sortedTimestamps.Data), 0}) // >= 0
}

func (b *SlidingWindowLogRateLimiter) Run(now time.Time, f func() error) error {
	threshold := now.Add(-b.duration)
	b.mu.Lock()
	b.sortedTimestamps.RemoveBelow(threshold.Unix()) // remove old timestamp
	usageCount := b.UsageCount()
	b.sortedTimestamps.Add(now.Unix()) // add regardless of whether the limit exceeds or not
	b.mu.Unlock()
	if usageCount == 0 {
		// exceed rate limit
		return fmt.Errorf("A limit has been exceeded: limit = %d", b.limit)
	}
	return f()
}
