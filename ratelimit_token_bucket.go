package ratelimiter

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type TokenBucketRateLimiter struct {
	bucketSize  int
	reloadCount int
	usageCount  int

	reloadCycleUnit time.Duration

	reloadCompletedCh chan int
	closeCh           chan struct{}

	mu sync.Mutex
}

func NewTokenBucketRateLimiter(limit int, reloadCount int, reloadCycleUnit time.Duration) RateLimiter {
	tokenBucket := TokenBucketRateLimiter{
		bucketSize:  limit,
		reloadCount: reloadCount,
		usageCount:  limit, // init UsageCount is full

		reloadCycleUnit: reloadCycleUnit,

		reloadCompletedCh: make(chan int),
		closeCh:           make(chan struct{}),
	}

	return &tokenBucket
}

func (b *TokenBucketRateLimiter) UsageCount() int {
	return b.usageCount
}

func (b *TokenBucketRateLimiter) Start() error {
	if b.bucketSize < 0 {
		return fmt.Errorf("bucket size must be positive")
	}
	if b.reloadCount < 0 {
		return fmt.Errorf("reload count must be positive")
	}
	go b.reloadCycle() // periodically reload the number of accessible
	return nil
}

func (b *TokenBucketRateLimiter) Close() {
	b.closeCh <- struct{}{}
}

func (b *TokenBucketRateLimiter) Run(f func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.usageCount > 0 {
		b.usageCount -= 1
	} else {
		return fmt.Errorf("A limit has been exceeded: limit = %d", b.bucketSize)
	}
	return f()
}

// reloadCycle periodically replenishes usage count
func (b *TokenBucketRateLimiter) reloadCycle() {
	ticker := time.NewTicker(b.reloadCycleUnit)
	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			b.usageCount = slices.Min([]int{b.usageCount + b.reloadCount, b.bucketSize}) // reload the number of accessible not to exceed bucket size
			b.mu.Unlock()
			b.reloadCompletedCh <- b.usageCount
		case <-b.closeCh:
			ticker.Stop()
			close(b.closeCh)
			return
		}
	}
}
