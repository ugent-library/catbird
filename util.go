package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	defaultPollFor      = 5 * time.Second
	defaultPollInterval = 200 * time.Millisecond
)

// CancelOpts configures cancellation behavior and metadata.
type CancelOpts struct {
	Reason string
}

// ScheduleOpts configures scheduled task/flow behavior.
type ScheduleOpts struct {
	Input any
}

// WaitOpts configures WaitForOutput polling behavior.
// Zero values use defaults.
type WaitOpts struct {
	PollFor      time.Duration
	PollInterval time.Duration
}

func resolvePollDurations(defaultPollFor, defaultPollInterval, pollFor, pollInterval time.Duration) (pollForMs int, pollIntervalMs int) {
	resolvedPollFor := defaultPollFor
	resolvedPollInterval := defaultPollInterval

	if pollFor > 0 {
		resolvedPollFor = pollFor
	}

	if pollInterval > 0 {
		resolvedPollInterval = pollInterval
	}

	if resolvedPollFor < time.Millisecond {
		resolvedPollFor = time.Millisecond
	}

	if resolvedPollInterval < time.Millisecond {
		resolvedPollInterval = time.Millisecond
	}

	if resolvedPollInterval >= resolvedPollFor {
		resolvedPollInterval = resolvedPollFor / 2
		if resolvedPollInterval < time.Millisecond {
			resolvedPollInterval = time.Millisecond
		}
	}

	return int(resolvedPollFor / time.Millisecond), int(resolvedPollInterval / time.Millisecond)
}

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
	if minDelay <= 0 || maxDelay <= 0 {
		return time.Millisecond
	}
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

func marshalPayloads(payloads []any) (pgtype.FlatArray[json.RawMessage], error) {
	encodedPayloads := make(pgtype.FlatArray[json.RawMessage], 0, len(payloads))
	for _, payload := range payloads {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		encodedPayloads = append(encodedPayloads, json.RawMessage(b))
	}

	return encodedPayloads, nil
}

// marshalOptionalHeaders marshals an optional headers object to JSON.
// Nil input maps to SQL NULL.
func marshalOptionalHeaders(headers map[string]any) (json.RawMessage, error) {
	if headers == nil {
		return nil, nil
	}

	b, err := json.Marshal(headers)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(b), nil
}

// marshalOptionalHeadersArray marshals optional per-message headers to JSON.
// Nil input maps to SQL NULL, and nil entries map to NULL headers for corresponding messages.
func marshalOptionalHeadersArray(headers []map[string]any) (pgtype.FlatArray[json.RawMessage], error) {
	if headers == nil {
		return nil, nil
	}

	encodedHeaders := make(pgtype.FlatArray[json.RawMessage], 0, len(headers))
	for _, header := range headers {
		if header == nil {
			encodedHeaders = append(encodedHeaders, nil)
			continue
		}

		b, err := json.Marshal(header)
		if err != nil {
			return nil, err
		}
		encodedHeaders = append(encodedHeaders, json.RawMessage(b))
	}

	return encodedHeaders, nil
}
