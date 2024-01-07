package ratelimiter

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlidingWindowLogRateLimiter_Run(t *testing.T) {

	type executionCount struct {
		runCount   int64 // success
		errorCount int64 // rate limit exceed count
	}

	t.Run("single test", func(t *testing.T) {
		now := time.Now()
		duration := 30 * time.Minute
		limit := 3
		inputs := []struct {
			timestamp time.Time
			ok        bool // whether limit exceeds or not
		}{
			{
				timestamp: now.Add(-50 * time.Minute),
				ok:        true,
			},
			{
				timestamp: now.Add(-45 * time.Minute),
				ok:        true,
			},
			{
				timestamp: now.Add(-40 * time.Minute),
				ok:        true,
			},
			{
				timestamp: now.Add(-35 * time.Minute),
				ok:        false,
			},
			{
				timestamp: now.Add(-30 * time.Minute),
				ok:        false,
			},
			{
				timestamp: now.Add(-5 * time.Minute),
				ok:        true,
			},
			{
				timestamp: now.Add(-3 * time.Minute),
				ok:        true,
			},
			{
				timestamp: now.Add(-1 * time.Minute),
				ok:        false,
			},
		}
		rateLimiter := NewSlidingWindowLogRateLimiter(limit, duration)

		for _, in := range inputs {
			err := rateLimiter.Run(in.timestamp, func() error {
				require.True(t, in.ok, in.timestamp)
				return nil
			})
			if err != nil {
				// exceed rate limit
				require.False(t, in.ok, in.timestamp)
			}
		}
	})

	t.Run("concurrent test", func(t *testing.T) {
		duration := time.Minute
		limit := 10

		var inputs []time.Time
		for i := 0; i < 20; i++ {
			inputs = append(inputs, time.Now())
		}

		forEach(100, func(_ int) {
			actual := executionCount{}
			rateLimiter := NewSlidingWindowLogRateLimiter(limit, duration)
			parallelTest(t, inputs, func(v time.Time) {
				err := rateLimiter.Run(v, func() error {
					atomic.AddInt64(&actual.runCount, 1)
					return nil
				})
				if err != nil {
					atomic.AddInt64(&actual.errorCount, 1)
				}
			})
			require.Equal(t, int64(10), actual.runCount)
			require.Equal(t, int64(10), actual.errorCount)
		})
	})
}
