package catbird

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func ptrOrNil[T comparable](t T) *T {
	var tt T
	if t == tt {
		return nil
	}
	return &t
}

// backoffWithFullJitter calculates the next backoff duration using full jitter strategy.
// The first retryAttempt should be 0.
// Always returns at least 1 millisecond to ensure database operations succeed.
func backoffWithFullJitter(retryAttempt int, minDelay, maxDelay time.Duration) time.Duration {
	delay := time.Duration(float64(minDelay) * math.Pow(2, float64(retryAttempt)))
	if delay > maxDelay {
		delay = maxDelay
	}
	jittered := time.Duration(rand.Int63n(int64(delay)))
	if jittered < time.Millisecond {
		return time.Millisecond
	}
	return jittered
}

// execWithRetry wraps conn.Exec with retry logic for transient errors
func execWithRetry(ctx context.Context, conn Conn, sql string, arguments ...any) (pgconn.CommandTag, error) {
	var lastErr error
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		tag, err := conn.Exec(ctx, sql, arguments...)
		if err == nil {
			return tag, nil
		}

		// Don't retry on context cancellation
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return tag, err
		}

		lastErr = err

		if attempt < maxAttempts-1 {
			delay := backoffWithFullJitter(attempt, 100*time.Millisecond, 10*time.Second)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return tag, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return pgconn.CommandTag{}, lastErr
}

// queryWithRetry wraps conn.Query with retry logic for transient errors
func queryWithRetry(ctx context.Context, conn Conn, sql string, arguments ...any) (pgx.Rows, error) {
	var lastErr error
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		rows, err := conn.Query(ctx, sql, arguments...)
		if err == nil {
			return rows, nil
		}

		// Don't retry on context cancellation
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		lastErr = err

		if attempt < maxAttempts-1 {
			delay := backoffWithFullJitter(attempt, 100*time.Millisecond, 10*time.Second)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, lastErr
}
