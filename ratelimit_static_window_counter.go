package ratelimiter

import (
	"sync"
	"time"
)

type StaticWindowCounterRateLimiter struct {
	limit       int
	reloadCount int
	usageCount  int

	reloadCycleUnit time.Duration

	reloadCompletedCh chan int
	closeCh           chan struct{}

	mu sync.Mutex
}

func NewStaticWindowCounterRateLimiter(limit int, reloadCycleUnit time.Duration) RateLimiter {
	return NewTokenBucketRateLimiter(limit, limit, reloadCycleUnit)
}
