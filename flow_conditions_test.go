package catbird

import (
	"context"
	"crypto/sha1"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var testRunID = fmt.Sprintf("%x", sha1.Sum([]byte(strconv.FormatInt(time.Now().UnixNano(), 10))))[:6]

func testFlowName(t *testing.T, base string) string {
	sanitized := strings.ToLower(t.Name())
	sanitized = strings.NewReplacer("/", "_").Replace(sanitized)
	sanitized = regexp.MustCompile(`[^a-z0-9_]+`).ReplaceAllString(sanitized, "_")
	suffix := fmt.Sprintf("%x", sha1.Sum([]byte(testRunID+"_"+sanitized)))[:8]
	maxBaseLen := 58 - len(suffix) - 1
	if maxBaseLen < 1 {
		maxBaseLen = 1
	}
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
	}
	return fmt.Sprintf("%s_%s", base, suffix)
}

// TestConditionEvaluation tests the cb_evaluate_condition() PostgreSQL function directly
func TestConditionEvaluation(t *testing.T) {
	client := getTestClient(t)

	tests := []struct {
		name       string
		condition  string
		payload    string
		namedConds map[string]string
		want       bool
		wantErr    bool
	}{
		// Basic equality
		{
			name:      "eq_number_true",
			condition: "score eq 80",
			payload:   `{"score": 80}`,
			want:      true,
		},
		{
			name:      "eq_number_false",
			condition: "score eq 80",
			payload:   `{"score": 90}`,
			want:      false,
		},
		{
			name:      "eq_string_true",
			condition: "status eq approved",
			payload:   `{"status": "approved"}`,
			want:      true,
		},
		{
			name:      "eq_string_false",
			condition: "status eq approved",
			payload:   `{"status": "rejected"}`,
			want:      false,
		},

		// Not equal
		{
			name:      "ne_true",
			condition: "status ne approved",
			payload:   `{"status": "pending"}`,
			want:      true,
		},
		{
			name:      "ne_false",
			condition: "status ne approved",
			payload:   `{"status": "approved"}`,
			want:      false,
		},

		// Greater than / less than
		{
			name:      "gt_true",
			condition: "score gt 80",
			payload:   `{"score": 90}`,
			want:      true,
		},
		{
			name:      "gt_false",
			condition: "score gt 80",
			payload:   `{"score": 70}`,
			want:      false,
		},
		{
			name:      "gte_equal",
			condition: "score gte 80",
			payload:   `{"score": 80}`,
			want:      true,
		},
		{
			name:      "gte_greater",
			condition: "score gte 80",
			payload:   `{"score": 90}`,
			want:      true,
		},
		{
			name:      "lt_true",
			condition: "score lt 80",
			payload:   `{"score": 70}`,
			want:      true,
		},
		{
			name:      "lte_equal",
			condition: "score lte 80",
			payload:   `{"score": 80}`,
			want:      true,
		},

		// Exists
		{
			name:      "exists_true",
			condition: "optional_field exists",
			payload:   `{"optional_field": "value"}`,
			want:      true,
		},
		{
			name:      "exists_false",
			condition: "optional_field exists",
			payload:   `{"other_field": "value"}`,
			want:      false,
		},
		{
			name:      "not_exists_true",
			condition: "not optional_field exists",
			payload:   `{"other_field": "value"}`,
			want:      true,
		},
		{
			name:      "not_exists_false",
			condition: "not optional_field exists",
			payload:   `{"optional_field": "value"}`,
			want:      false,
		},

		// Contains
		{
			name:      "contains_string_true",
			condition: "message contains error",
			payload:   `{"message": "an error occurred"}`,
			want:      true,
		},
		{
			name:      "contains_string_false",
			condition: "message contains error",
			payload:   `{"message": "success"}`,
			want:      false,
		},
		{
			name:      "not_contains_true",
			condition: "not message contains error",
			payload:   `{"message": "success"}`,
			want:      true,
		},
		{
			name:      "not_contains_false",
			condition: "not message contains error",
			payload:   `{"message": "an error occurred"}`,
			want:      false,
		},

		// Nested fields
		{
			name:      "nested_field_true",
			condition: "user.age gte 18",
			payload:   `{"user": {"age": 25}}`,
			want:      true,
		},
		{
			name:      "nested_field_false",
			condition: "user.age gte 18",
			payload:   `{"user": {"age": 15}}`,
			want:      false,
		},
		{
			name:      "deep_nested_field",
			condition: "data.metrics.score gt 50",
			payload:   `{"data": {"metrics": {"score": 75}}}`,
			want:      true,
		},

		// Edge cases
		{
			name:      "missing_field_eq",
			condition: "missing eq value",
			payload:   `{"other": "data"}`,
			want:      false,
		},
		{
			name:      "null_value_exists",
			condition: "nullable exists",
			payload:   `{"nullable": null}`,
			want:      true, // Field exists even if null
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool

			query := `SELECT cb_evaluate_condition_expr($1, $2::jsonb)`
			err := client.Conn.QueryRow(t.Context(), query, tt.condition, tt.payload).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.want {
				t.Errorf("cb_evaluate_condition_expr(%q, %s) = %v, want %v",
					tt.condition, tt.payload, result, tt.want)
			}
		})
	}
}

// TestFlowCondition tests positive condition logic in flows
func TestFlowCondition(t *testing.T) {
	client := getTestClient(t)

	t.Run("skip_step_when_condition_false", func(t *testing.T) {
		// Flow: step1 -> step2 (conditional, executed if step1 output < 90) -> finalize (unconditional final step)
		// If input is 95, step1 outputs 95, condition is false, so step2 should be skipped, finalize still runs
		flowName := testFlowName(t, "condition_test_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
				return score, nil
			}, nil)).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1 lt 90").
				Handler(func(ctx context.Context, score int, step1Score int) (string, error) {
					return "step2_executed", nil
				}, nil)).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, score int, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				result["step1"] = score
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}, nil))

		worker, err := client.NewWorker(t.Context(), WithFlow(flow))
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}

		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, 95, nil)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		// step2 should not be in the output since it was skipped
		if _, exists := result["step2"]; exists {
			t.Errorf("step2 should have been skipped, but found in output: %v", result)
		}

		// Verify step2 was marked as 'skipped' in database
		var state string
		stepTableName := "cb_s_" + handle.Name
		query := fmt.Sprintf(`SELECT status FROM %s WHERE flow_run_id = $1 AND step_name = 'step2'`, stepTableName)
		err = client.Conn.QueryRow(t.Context(), query, handle.ID).Scan(&state)
		if err != nil {
			t.Fatalf("failed to query step state: %v", err)
		}
		if state != "skipped" {
			t.Errorf("step2 state = %q, want 'skipped'", state)
		}
	})

	t.Run("execute_step_when_condition_true", func(t *testing.T) {
		flowName := testFlowName(t, "condition_true_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
				return score, nil
			}, nil)).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1 lt 90").
				Handler(func(ctx context.Context, score int, step1Score int) (string, error) {
					return "step2_executed", nil
				}, nil)).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, score int, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				result["step1"] = score
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}, nil))

		worker, err := client.NewWorker(t.Context(), WithFlow(flow))
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, 75, nil)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		// step2 should have executed
		if step2Val, exists := result["step2"]; !exists {
			t.Errorf("step2 should have executed, but not found in output")
		} else if step2Val != "step2_executed" {
			t.Errorf("step2 output = %v, want 'step2_executed'", step2Val)
		}
	})

	t.Run("skip_with_condition", func(t *testing.T) {
		flowName := testFlowName(t, "condition_named_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
				return score, nil
			}, nil)).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1 lt 90").
				Handler(func(ctx context.Context, score int, step1Score int) (string, error) {
					return "step2_executed", nil
				}, nil)).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, score int, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				result["step1"] = score
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}, nil))

		worker, err := client.NewWorker(t.Context(), WithFlow(flow))
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, 95, nil)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		// Verify step2 was skipped
		var state string
		tableName := "cb_s_" + handle.Name
		query := fmt.Sprintf(`SELECT status FROM %s WHERE flow_run_id = $1 AND step_name = 'step2'`, tableName)
		err = client.Conn.QueryRow(t.Context(), query, handle.ID).Scan(&state)
		if err != nil {
			t.Fatalf("failed to query step state: %v", err)
		}
		if state != "skipped" {
			t.Errorf("step2 state = %q, want 'skipped'", state)
		}
	})
}

// TestFlowConditionEdgeCases tests edge cases in condition evaluation
func TestFlowConditionEdgeCases(t *testing.T) {
	client := getTestClient(t)

	t.Run("missing_field_condition", func(t *testing.T) {
		// When field doesn't exist, condition should be false
		flowName := testFlowName(t, "missing_field_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return input, nil
			}, nil)).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1.optional_field ne value").
				Handler(func(ctx context.Context, input map[string]interface{}, step1Out map[string]interface{}) (string, error) {
					return "executed", nil
				}, nil)).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, input map[string]interface{}, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}, nil))

		worker, err := client.NewWorker(t.Context(), WithFlow(flow))
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, map[string]interface{}{"other": "data"}, nil)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		// step2 should execute because condition is false (field missing)
		if _, exists := result["step2"]; !exists {
			t.Errorf("step2 should have executed when field is missing")
		}
	})

	t.Run("null_value_handling", func(t *testing.T) {
		flowName := testFlowName(t, "null_value_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return input, nil
			}, nil)).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1.field exists").
				Handler(func(ctx context.Context, input map[string]interface{}, step1Out map[string]interface{}) (string, error) {
					return "executed", nil
				}, nil)).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, input map[string]interface{}, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}, nil))

		worker, err := client.NewWorker(t.Context(), WithFlow(flow))
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, map[string]interface{}{"field": nil}, nil)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		// Field exists (even though it's null), so step2 should execute (condition is true)
		step2Val, exists := result["step2"]
		if !exists {
			t.Errorf("step2 should have executed when field exists (even if null)")
		} else if step2Val != "executed" {
			t.Errorf("step2 output = %v, want 'executed'", step2Val)
		}
	})

	t.Run("complex_nested_path", func(t *testing.T) {
		flowName := testFlowName(t, "nested_path_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return input, nil
			}, nil)).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1.data.user.age gte 18").
				Handler(func(ctx context.Context, input map[string]interface{}, step1Out map[string]interface{}) (string, error) {
					return "executed", nil
				}, nil)).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, input map[string]interface{}, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}, nil))

		worker, err := client.NewWorker(t.Context(), WithFlow(flow))
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, map[string]interface{}{
			"data": map[string]interface{}{
				"user": map[string]interface{}{
					"age": 15,
				},
			},
		}, nil)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		// step2 should be skipped because age < 18
		var state string
		tableName := "cb_s_" + handle.Name
		query := fmt.Sprintf(`SELECT status FROM %s WHERE flow_run_id = $1 AND step_name = 'step2'`, tableName)
		err = client.Conn.QueryRow(t.Context(), query, handle.ID).Scan(&state)
		if err != nil {
			t.Fatalf("failed to query step state: %v", err)
		}
		if state != "skipped" {
			t.Errorf("step2 state = %q, want 'skipped'", state)
		}
	})
}

func TestFlowOptionalDependency(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "optional_dep_flow")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
			return score, nil
		}, nil)).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Condition("step1 gte 50").
			Handler(func(ctx context.Context, score int, step1Score int) (int, error) {
				return step1Score * 2, nil
			}, nil)).
		AddStep(NewStep("step3").
			DependsOn("step2"). // TODO: Support OptionalDependency marker
			Handler(func(ctx context.Context, score int, step2Out Optional[int]) (int, error) {
				if step2Out.IsSet {
					return step2Out.Value, nil
				}
				return 0, nil
			}, nil))

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}
	startTestWorker(t, worker)

	// Step2 skipped
	handle, err := client.RunFlow(t.Context(), flowName, 40, nil)
	if err != nil {
		t.Fatalf("RunFlow failed: %v", err)
	}

	ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result int
	if err := handle.WaitForOutput(ctxTimeout, &result); err != nil {
		t.Fatalf("WaitForOutput failed: %v", err)
	}

	if result != 0 {
		t.Errorf("step3 output = %d, want 0", result)
	}

	// Step2 executed
	handle, err = client.RunFlow(t.Context(), flowName, 60, nil)
	if err != nil {
		t.Fatalf("RunFlow failed: %v", err)
	}

	ctxTimeout, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	result = 0
	if err := handle.WaitForOutput(ctxTimeout, &result); err != nil {
		t.Fatalf("WaitForOutput failed: %v", err)
	}

	if result != 120 {
		t.Errorf("step3 output = %d, want 120", result)
	}
}

func TestFlowConditionalDependencyRequiresOptional(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "missing_optional_dep")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
			return score, nil
		}, nil)).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Condition("step1 gte 50").
			Handler(func(ctx context.Context, score int, step1Score int) (int, error) {
				return step1Score * 2, nil
			}, nil)).
		AddStep(NewStep("step3").
			DependsOn("step2").
			Handler(func(ctx context.Context, score int, step2Out int) (int, error) {
				return step2Out, nil
			}, nil))

	if err := client.CreateFlow(t.Context(), flow); err == nil {
		t.Fatalf("expected CreateFlow to fail when depending on conditional step without OptionalDependency")
	}
}
