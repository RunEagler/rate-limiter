package ratelimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBucketRateLimiter_Run(t *testing.T) {

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

	runAndReload := func(rateLimiter RateLimiter, parallelCount int, actual *executionCount) {
		runParallel(rateLimiter, parallelCount, actual)                                                    // all passed
		err := waitReceivedChannel(rateLimiter.(*TokenBucketRateLimiter).reloadCompletedCh, 5*time.Second) // wait for reloading to bucket
		require.NoErrorf(t, err, "failed to reload count")
	}

	assertNoErrorRateLimitStart := func(err error) {
		require.Nil(t, err, "failed to start rate limit")
	}

	assertExecutionCounts := func(expect, actual *executionCount) {
		require.Equal(t, expect.runCount, actual.runCount, "not equal runCount")
		require.Equal(t, expect.errorCount, actual.errorCount, "not equal errorCount")
	}

	t.Run("success", func(t *testing.T) {
		t.Run("concurrent access is exceeded without reload count", func(t *testing.T) {
			forEach(100, func(_ int) {
				rateLimiter := NewTokenBucketRateLimiter(100, 0, time.Second)
				expect := executionCount{
					runCount:   100,
					errorCount: 50,
				}
				actual := executionCount{}

				// run test
				err := rateLimiter.Start()
				assertNoErrorRateLimitStart(err)

				runParallel(rateLimiter, 150, &actual)
				assertExecutionCounts(&expect, &actual)
			})
		})

		t.Run("reload test", func(t *testing.T) {
			forEach(100, func(_ int) {
				reloadCycle := 1 * time.Millisecond
				rateLimiter := NewTokenBucketRateLimiter(100, 10, reloadCycle)
				expect := executionCount{
					runCount:   90 + 20,
					errorCount: 10,
				}
				actual := executionCount{}

				// run test
				err := rateLimiter.Start()
				assertNoErrorRateLimitStart(err)

				runAndReload(rateLimiter, 90, &actual)
				runAndReload(rateLimiter, 30, &actual)

				assertExecutionCounts(&expect, &actual)

				rateLimiter.Close()
			})
		})

		t.Run("the reload count exceed bucket size", func(t *testing.T) {
			forEach(100, func(_ int) {
				reloadCycle := time.Millisecond
				rateLimiter := NewTokenBucketRateLimiter(10, 100, reloadCycle)
				expect := executionCount{
					runCount:   10 + 10,
					errorCount: 90 + 90,
				}
				actual := executionCount{}

				// run test
				err := rateLimiter.Start()
				assertNoErrorRateLimitStart(err)
				runAndReload(rateLimiter, 100, &actual) // only 10 cases passed, the rest failed
				runAndReload(rateLimiter, 100, &actual) // only 10 cases passed, the rest failed

				assertExecutionCounts(&expect, &actual)
			})
		})
	})

	t.Run("error", func(t *testing.T) {
		t.Run("limit is negative", func(t *testing.T) {
			reloadCycle := time.Millisecond
			rateLimiter := NewTokenBucketRateLimiter(-1, 10, reloadCycle)

			// run test
			err := rateLimiter.Start()
			require.Error(t, err, "failed to start rate limit")
		})

		t.Run("reload count is negative", func(t *testing.T) {
			reloadCycle := time.Millisecond
			rateLimiter := NewTokenBucketRateLimiter(10, -1, reloadCycle)

			// run test
			err := rateLimiter.Start()
			require.Error(t, err, "failed to start rate limit")
		})
	})
}
