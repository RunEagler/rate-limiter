package ratelimiter

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type RateLimiter interface {
	Run(f func() error) error
	Start() error
	Close()
	UsageCount() int
}

type LeakyBucketRateLimiter struct {
	queue         []chan struct{}
	queueSize     int
	flowBatchSize int
	usageCount    int

	flowRate   time.Duration
	runTimeout time.Duration

	subscribeCompletedCh chan int
	closeCh              chan struct{}

	mu sync.RWMutex
}

func NewLeakyBucketRateLimiter(limit int, flowBatchSize int, flowRate time.Duration) RateLimiter {
	tokenBucket := LeakyBucketRateLimiter{
		queue:         make([]chan struct{}, 0),
		queueSize:     limit,
		flowBatchSize: flowBatchSize,
		usageCount:    limit,

		flowRate:   flowRate,
		runTimeout: 30 * time.Second,

		subscribeCompletedCh: make(chan int),
		closeCh:              make(chan struct{}),
	}

	return &tokenBucket
}

func (b *LeakyBucketRateLimiter) UsageCount() int {
	return b.usageCount
}

func (b *LeakyBucketRateLimiter) Run(f func() error) error {
	var ch chan struct{}

	b.mu.Lock()
	if b.usageCount > 0 {
		// can enqueue
		b.usageCount -= 1
		b.mu.Unlock()
		ch = make(chan struct{}, 1)
		b.enqueue(ch)
	} else {
		// failed to enqueue
		b.mu.Unlock()
		return fmt.Errorf("A limit has been exceeded: limit = %d", b.queueSize)
	}
	select {
	case <-ch:
		return f()
	case <-time.After(b.flowRate + b.runTimeout):
		return fmt.Errorf("run timeout")
	}
}

func (b *LeakyBucketRateLimiter) Start() error {
	if b.queueSize < 0 {
		return fmt.Errorf("queue size must be positive")
	}
	if b.flowBatchSize < 0 {
		return fmt.Errorf("flow batch size must be positive")
	}
	go b.subscribeCycle() // periodically reload the number of accessible
	return nil
}

func (b *LeakyBucketRateLimiter) Close() {
	b.closeCh <- struct{}{}
}

func (b *LeakyBucketRateLimiter) enqueue(ch chan struct{}) {
	b.mu.Lock()
	b.queue = append(b.queue, ch)
	b.mu.Unlock()
}

func (b *LeakyBucketRateLimiter) dequeue() chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	lastIndex := len(b.queue) - 1
	last := b.queue[lastIndex]
	b.queue = b.queue[:lastIndex]
	return last
}

func (b *LeakyBucketRateLimiter) subscribe() {
	b.mu.RLock()
	dequeueCount := slices.Min([]int{len(b.queue), b.flowBatchSize}) // dequeue count that does not exceed queue size
	b.mu.RUnlock()
	for i := int(0); i < dequeueCount; i++ {
		ch := b.dequeue()
		b.mu.Lock()
		b.usageCount++
		b.mu.Unlock()
		ch <- struct{}{} // start run
	}
}

func (b *LeakyBucketRateLimiter) subscribeCycle() {
	ticker := time.NewTicker(b.flowRate)
	for {
		select {
		case <-ticker.C:
			b.subscribe()
		case <-b.closeCh:
			// stop timer
			ticker.Stop()
			close(b.closeCh)
			return
		}
	}
}
