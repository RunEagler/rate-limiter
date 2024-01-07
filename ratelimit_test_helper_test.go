package ratelimiter

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func forEach(trialCount int, f func(i int)) {
	for i := 0; i < trialCount; i++ {
		f(i)
	}
}

func waitReceivedChannel[T any](ch chan T, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout received channel")
	}
}

func waitGroupWithTimeout(wg *sync.WaitGroup, timeout time.Duration, msgs ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		// done wait group
		return nil
	case <-ctx.Done():
		// timeout
		return fmt.Errorf("timeout wait group : %s", messageFromMsgs(msgs...))
	}
}

func parallelTest[T any](t *testing.T, values []T, f func(value T), messages ...string) {
	wg := sync.WaitGroup{}
	startWg := sync.WaitGroup{}
	wg.Add(len(values))
	startWg.Add(1)
	for _, value := range values {
		go func(value T) {
			startWg.Wait()
			f(value)
			wg.Done()
		}(value)
	}
	startWg.Done()
	err := waitGroupWithTimeout(&wg, 5*time.Second, messages)
	require.NoError(t, err)
}

func messageFromMsgs(msgAndArgs ...interface{}) string {
	if len(msgAndArgs) == 0 || msgAndArgs == nil {
		return ""
	}
	if len(msgAndArgs) == 1 {
		msg := msgAndArgs[0]
		if msgAsStr, ok := msg.(string); ok {
			return msgAsStr
		}
		return fmt.Sprintf("%+v", msg)
	}
	if len(msgAndArgs) > 1 {
		return fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
	}
	return ""
}
