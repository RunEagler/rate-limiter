package ratelimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeakyBucketRateLimiter_Run(t *testing.T) {

	type executionCount struct {
		runCount   int64 // success
		errorCount int64 // rate limit exceed count
	}

	runParallel := func(rateLimiter RateLimiter, parallelCount int, actual *executionCount) {
		startWg := sync.WaitGroup{}
		doneWg := sync.WaitGroup{}
		startWg.Add(1)
		doneWg.Add(parallelCount)
		for i := 0; i < parallelCount; i++ {
			go func() {
				startWg.Wait() // wait Start
				err := rateLimiter.Run(func() error {
					// success run
					atomic.AddInt64(&actual.runCount, 1)
					doneWg.Done()
					return nil
				})
				if err != nil {
					// exceed rate limit
					atomic.AddInt64(&actual.errorCount, 1)
					doneWg.Done()
				}
			}()
		}
		startWg.Done() // Start concurrent execution
		doneWg.Wait()  // wait until all is complete
	}

	assertExecutionCounts := func(expect, actual *executionCount) {
		require.Equal(t, expect.runCount, actual.runCount, "not equal runCount")
		require.Equal(t, expect.errorCount, actual.errorCount, "not equal errorCount")
	}

	t.Run("success", func(t *testing.T) {
		t.Run("passed only for the queue size without affecting flow batch size", func(t *testing.T) {
			multipleTest(100, func() {
				for flowBatchSize := 1; flowBatchSize < 5; flowBatchSize++ {
					rateLimiter := NewLeakyBucketRateLimiter(5, flowBatchSize, 100*time.Microsecond)
					expect := executionCount{
						runCount:   5,
						errorCount: 5,
					}
					actual := executionCount{}

					err := rateLimiter.Start()
					require.NoError(t, err)

					// run test
					runParallel(rateLimiter, 10, &actual)

					rateLimiter.Close() // stop subscribe timer

					assertExecutionCounts(&expect, &actual)
				}
			})
		})

		t.Run("even if flow batch size is larger than queue size, processed as queue size limit", func(t *testing.T) {
			multipleTest(100, func() {
				rateLimiter := NewLeakyBucketRateLimiter(5, 100, 100*time.Microsecond)
				expect := executionCount{
					runCount:   5 + 5 + 5,
					errorCount: 5,
				}
				actual := executionCount{}

				err := rateLimiter.Start()
				require.NoError(t, err)

				// run test
				runParallel(rateLimiter, 5, &actual)
				runParallel(rateLimiter, 5, &actual)
				runParallel(rateLimiter, 10, &actual)

				rateLimiter.Close() // stop subscribe timer

				assertExecutionCounts(&expect, &actual)
			})
		})
	})

	t.Run("error", func(t *testing.T) {
		t.Run("limit is negative", func(t *testing.T) {
			rateLimiter := NewLeakyBucketRateLimiter(-1, 10, time.Millisecond)

			// run test
			err := rateLimiter.Start()
			require.Error(t, err, "failed to start rate limit")
		})

		t.Run("flow batch size is negative", func(t *testing.T) {
			rateLimiter := NewLeakyBucketRateLimiter(10, -1, time.Millisecond)

			// run test
			err := rateLimiter.Start()
			require.Error(t, err, "failed to start rate limit")
		})
	})
}
