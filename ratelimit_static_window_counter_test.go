package ratelimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewStaticWindowLogRateLimiter(t *testing.T) {
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
		runParallel(rateLimiter, parallelCount, actual)                                     // all passed
		err := waitReceivedChannel(rateLimiter.(*TokenBucketRateLimiter).reloadCompletedCh) // wait for reloading to bucket
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
			multipleTest(100, func() {
				rateLimiter := NewStaticWindowCounterRateLimiter(100, time.Second)
				expect := executionCount{
					runCount:   100,
					errorCount: 50,
				}
				actual := executionCount{}

				// run test
				err := rateLimiter.Start()
				assertNoErrorRateLimitStart(err)
				runParallel(rateLimiter, 150, &actual)

				require.Equal(t, 0, rateLimiter.UsageCount())
				assertExecutionCounts(&expect, &actual)
			})
		})

		t.Run("reload test", func(t *testing.T) {
			multipleTest(100, func() {
				reloadCycle := 1 * time.Millisecond
				rateLimiter := NewStaticWindowCounterRateLimiter(100, reloadCycle)
				expect := executionCount{
					runCount: 90 + 30,
				}
				actual := executionCount{}

				// run test
				err := rateLimiter.Start()
				assertNoErrorRateLimitStart(err)

				runAndReload(rateLimiter, 90, &actual)
				runParallel(rateLimiter, 30, &actual) // not reload

				require.Equal(t, 70, rateLimiter.UsageCount())
				assertExecutionCounts(&expect, &actual)

				rateLimiter.Close()
			})
		})

		t.Run("the reload count exceed bucket size", func(t *testing.T) {
			multipleTest(100, func() {
				reloadCycle := time.Millisecond
				rateLimiter := NewStaticWindowCounterRateLimiter(10, reloadCycle)
				expect := executionCount{
					runCount:   10 + 10,
					errorCount: 90 + 90,
				}
				actual := executionCount{}

				// run test
				err := rateLimiter.Start()
				assertNoErrorRateLimitStart(err)
				runAndReload(rateLimiter, 100, &actual) // only 10 cases passed, the rest failed
				runParallel(rateLimiter, 100, &actual)  // only 10 cases passed, the rest failed

				require.Equal(t, 0, rateLimiter.UsageCount())
				assertExecutionCounts(&expect, &actual)
			})
		})
	})

	t.Run("error", func(t *testing.T) {
		t.Run("limit is negative", func(t *testing.T) {
			reloadCycle := time.Millisecond
			rateLimiter := NewStaticWindowCounterRateLimiter(-1, reloadCycle)

			// run test
			err := rateLimiter.Start()
			require.Error(t, err, "failed to start rate limit")
		})
	})
}
