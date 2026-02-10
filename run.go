package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RunInfo represents the details of a task or flow execution
type RunInfo struct {
	ID              int64           `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
	Input           json.RawMessage `json:"input,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	StartedAt       time.Time       `json:"started_at,omitzero"`
	CompletedAt     time.Time       `json:"completed_at,omitzero"`
	FailedAt        time.Time       `json:"failed_at,omitzero"`
}

// OutputAs unmarshals the output of a completed run.
// Returns an error if the run has failed or is not completed yet.
func (r *RunInfo) OutputAs(out any) error {
	if r.Status == StatusFailed {
		return fmt.Errorf("%w: %s", ErrRunFailed, r.ErrorMessage)
	}
	if r.Status != StatusCompleted {
		return fmt.Errorf("run not completed: current status is %s", r.Status)
	}
	return json.Unmarshal(r.Output, out)
}

// RunHandle is a handle to a task or flow execution
type RunHandle struct {
	conn  Conn
	getFn func(context.Context, Conn, string, int64) (*RunInfo, error)
	Name  string
	ID    int64
}

// WaitForOutput blocks until the task or flow execution completes and unmarshals the output
func (h *RunHandle) WaitForOutput(ctx context.Context, out any) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := h.getFn(ctx, h.conn, h.Name, h.ID)
			if err != nil {
				return err
			}
			switch info.Status {
			case StatusCompleted:
				return json.Unmarshal(info.Output, out)
			case StatusFailed:
				return fmt.Errorf("%w: %s", ErrRunFailed, info.ErrorMessage)
			}
		}
	}
}

func scanCollectibleRun(row pgx.CollectableRow) (*RunInfo, error) {
	return scanRun(row)
}

func scanRun(row pgx.Row) (*RunInfo, error) {
	rec := RunInfo{}

	var deduplicationID *string
	var input *json.RawMessage
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
		&input,
		&output,
		&errorMessage,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		return nil, err
	}

	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}
	if input != nil {
		rec.Input = *input
	}
	if output != nil {
		rec.Output = *output
	}
	if errorMessage != nil {
		rec.ErrorMessage = *errorMessage
	}
	if completedAt != nil {
		rec.CompletedAt = *completedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}

	return &rec, nil
}
