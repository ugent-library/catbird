package catbird

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const benchmarkPipelineBatchSize = 64
const benchmarkQueueBatchSize = 256
const benchmarkTunedPipelineBatchSize = 512

var benchmarkTunedHandlerOpts = []HandlerOpt{
	WithConcurrency(32),
	WithBatchSize(256),
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func waitForTaskCompletedCount(b *testing.B, client *Client, taskName string, want int) {
	b.Helper()

	tableName := fmt.Sprintf("cb_t_%s", taskName)
	q := fmt.Sprintf(`SELECT count(*) FROM %s WHERE status = 'completed';`, pgx.Identifier{tableName}.Sanitize())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		var completed int
		if err := client.Conn.QueryRow(ctx, q).Scan(&completed); err != nil {
			b.Fatal(err)
		}

		if completed >= want {
			return
		}

		if ctx.Err() != nil {
			b.Fatalf("timed out waiting for completed task runs: got %d, want %d", completed, want)
		}

		time.Sleep(time.Millisecond)
	}
}

func startBenchmarkWorker(b *testing.B, worker *Worker) {
	b.Helper()
	worker.shutdownTimeout = 0

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- worker.Start(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			b.Fatalf("worker.Start() failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}

	b.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				b.Logf("worker shutdown error: %v", err)
			}
		case <-time.After(10 * time.Second):
			b.Log("worker did not shutdown within timeout")
		}
	})
}

func BenchmarkQueueThroughput(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	queueName := fmt.Sprintf("bench_queue_%d", time.Now().UnixNano())
	if err := client.CreateQueue(ctx, queueName); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, queueName)
	})

	type payload struct {
		Value int `json:"value"`
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Send(ctx, queueName, payload{Value: i}); err != nil {
			b.Fatal(err)
		}

		msgs, err := client.Read(ctx, queueName, 1, time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) != 1 {
			b.Fatalf("expected 1 message, got %d", len(msgs))
		}

		if _, err := client.Delete(ctx, queueName, msgs[0].ID); err != nil {
			b.Fatal(err)
		}
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "msg/s")
	}
}

func BenchmarkTaskThroughput(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	taskName := fmt.Sprintf("bench_task_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})

	worker := client.NewWorker(ctx).AddTask(task)
	startBenchmarkWorker(b, worker)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, err := client.RunTask(ctx, taskName, i)
		if err != nil {
			b.Fatal(err)
		}

		var out int
		if err := h.WaitForOutput(ctx, &out, WaitOpts{PollFor: 30 * time.Second, PollInterval: time.Millisecond}); err != nil {
			b.Fatal(err)
		}
		if out != i+1 {
			b.Fatalf("unexpected task output: got %d, want %d", out, i+1)
		}
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "task_runs/s")
	}
}

func BenchmarkFlowThroughput(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	flowName := fmt.Sprintf("bench_flow_%d", time.Now().UnixNano())
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in int) (int, error) {
			return in + 1, nil
		}))

	worker := client.NewWorker(ctx).AddFlow(flow)
	startBenchmarkWorker(b, worker)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, err := client.RunFlow(ctx, flowName, i)
		if err != nil {
			b.Fatal(err)
		}

		var out int
		if err := h.WaitForOutput(ctx, &out, WaitOpts{PollFor: 30 * time.Second, PollInterval: time.Millisecond}); err != nil {
			b.Fatal(err)
		}
		if out != i+1 {
			b.Fatalf("unexpected flow output: got %d, want %d", out, i+1)
		}
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "flow_runs/s")
	}
}

func BenchmarkTaskThroughputPipelined(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	taskName := fmt.Sprintf("bench_task_pipelined_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})

	worker := client.NewWorker(ctx).AddTask(task)
	startBenchmarkWorker(b, worker)

	waitOpts := WaitOpts{PollFor: 30 * time.Second, PollInterval: time.Millisecond}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := minInt(benchmarkPipelineBatchSize, b.N-i)
		handles := make([]*TaskHandle, 0, batchSize)

		for j := 0; j < batchSize; j++ {
			input := i + j
			h, err := client.RunTask(ctx, taskName, input)
			if err != nil {
				b.Fatal(err)
			}
			handles = append(handles, h)
		}

		for j, h := range handles {
			var out int
			if err := h.WaitForOutput(ctx, &out, waitOpts); err != nil {
				b.Fatal(err)
			}
			want := i + j + 1
			if out != want {
				b.Fatalf("unexpected task output: got %d, want %d", out, want)
			}
		}

		i += batchSize
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "task_runs/s")
	}
}

func BenchmarkQueueThroughputBatched(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	queueName := fmt.Sprintf("bench_queue_batched_%d", time.Now().UnixNano())
	if err := client.CreateQueue(ctx, queueName); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, queueName)
	})

	type payload struct {
		Value int `json:"value"`
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := minInt(benchmarkQueueBatchSize, b.N-i)

		payloads := make([]any, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			payloads = append(payloads, payload{Value: i + j})
		}

		if err := client.SendMany(ctx, queueName, payloads); err != nil {
			b.Fatal(err)
		}

		ids := make([]int64, 0, batchSize)
		for len(ids) < batchSize {
			remaining := batchSize - len(ids)
			msgs, err := client.Read(ctx, queueName, remaining, time.Minute)
			if err != nil {
				b.Fatal(err)
			}
			if len(msgs) == 0 {
				b.Fatalf("expected messages after SendMany, got none (have=%d want=%d)", len(ids), batchSize)
			}

			for _, msg := range msgs {
				ids = append(ids, msg.ID)
			}
		}

		if err := client.DeleteMany(ctx, queueName, ids); err != nil {
			b.Fatal(err)
		}

		i += batchSize
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "msg/s")
	}
}

func BenchmarkFlowThroughputPipelined(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	flowName := fmt.Sprintf("bench_flow_pipelined_%d", time.Now().UnixNano())
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in int) (int, error) {
			return in + 1, nil
		}))

	worker := client.NewWorker(ctx).AddFlow(flow)
	startBenchmarkWorker(b, worker)

	waitOpts := WaitOpts{PollFor: 30 * time.Second, PollInterval: time.Millisecond}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := minInt(benchmarkPipelineBatchSize, b.N-i)
		handles := make([]*FlowHandle, 0, batchSize)

		for j := 0; j < batchSize; j++ {
			input := i + j
			h, err := client.RunFlow(ctx, flowName, input)
			if err != nil {
				b.Fatal(err)
			}
			handles = append(handles, h)
		}

		for j, h := range handles {
			var out int
			if err := h.WaitForOutput(ctx, &out, waitOpts); err != nil {
				b.Fatal(err)
			}
			want := i + j + 1
			if out != want {
				b.Fatalf("unexpected flow output: got %d, want %d", out, want)
			}
		}

		i += batchSize
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "flow_runs/s")
	}
}

func BenchmarkTaskThroughputPipelinedTuned(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	taskName := fmt.Sprintf("bench_task_pipelined_tuned_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	}, benchmarkTunedHandlerOpts...)

	worker := client.NewWorker(ctx).AddTask(task)
	startBenchmarkWorker(b, worker)

	waitOpts := WaitOpts{PollFor: 30 * time.Second, PollInterval: time.Millisecond}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := minInt(benchmarkTunedPipelineBatchSize, b.N-i)
		handles := make([]*TaskHandle, 0, batchSize)

		for j := 0; j < batchSize; j++ {
			input := i + j
			h, err := client.RunTask(ctx, taskName, input)
			if err != nil {
				b.Fatal(err)
			}
			handles = append(handles, h)
		}

		for j, h := range handles {
			var out int
			if err := h.WaitForOutput(ctx, &out, waitOpts); err != nil {
				b.Fatal(err)
			}
			want := i + j + 1
			if out != want {
				b.Fatalf("unexpected task output: got %d, want %d", out, want)
			}
		}

		i += batchSize
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "task_runs/s")
	}
}

func BenchmarkTaskEngineThroughputTuned(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	taskName := fmt.Sprintf("bench_task_engine_tuned_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	}, benchmarkTunedHandlerOpts...)

	worker := client.NewWorker(ctx).AddTask(task)
	startBenchmarkWorker(b, worker)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := minInt(benchmarkTunedPipelineBatchSize, b.N-i)
		for j := 0; j < batchSize; j++ {
			if _, err := client.RunTask(ctx, taskName, i+j); err != nil {
				b.Fatal(err)
			}
		}

		i += batchSize
	}

	waitForTaskCompletedCount(b, client, taskName, b.N)

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "task_runs/s")
	}
}

func BenchmarkFlowThroughputPipelinedTuned(b *testing.B) {
	client := getTestClient(b)
	ctx := context.Background()

	flowName := fmt.Sprintf("bench_flow_pipelined_tuned_%d", time.Now().UnixNano())
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in int) (int, error) {
			return in + 1, nil
		}, benchmarkTunedHandlerOpts...))

	worker := client.NewWorker(ctx).AddFlow(flow)
	startBenchmarkWorker(b, worker)

	waitOpts := WaitOpts{PollFor: 30 * time.Second, PollInterval: time.Millisecond}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := minInt(benchmarkTunedPipelineBatchSize, b.N-i)
		handles := make([]*FlowHandle, 0, batchSize)

		for j := 0; j < batchSize; j++ {
			input := i + j
			h, err := client.RunFlow(ctx, flowName, input)
			if err != nil {
				b.Fatal(err)
			}
			handles = append(handles, h)
		}

		for j, h := range handles {
			var out int
			if err := h.WaitForOutput(ctx, &out, waitOpts); err != nil {
				b.Fatal(err)
			}
			want := i + j + 1
			if out != want {
				b.Fatalf("unexpected flow output: got %d, want %d", out, want)
			}
		}

		i += batchSize
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "flow_runs/s")
	}
}
