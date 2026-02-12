package dashboard

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jackc/pgx/v5"
	"github.com/ugent-library/catbird"
)

//go:embed *.html *.css
var templatesFS embed.FS

type App struct {
	client  *catbird.Client
	logger  *slog.Logger
	index   *template.Template
	queues  *template.Template
	tasks   *template.Template
	task    *template.Template
	flows   *template.Template
	flow    *template.Template
	workers *template.Template
}

// Config configures the dashboard web application.
type Config struct {
	// Client is a Catbird client connected to your database. Required.
	Client *catbird.Client

	// Log is a custom logger for dashboard operations. Optional.
	// If nil, uses slog.Default().
	Log *slog.Logger

	// PathPrefix is the URL prefix for constructing internal links.
	// This should match your routing setup. Optional.
	//
	// When mounting at root or using http.StripPrefix, leave empty.
	// Example: if you mount at "/admin/catbird/" using http.StripPrefix,
	// set PathPrefix to "" (the prefix is already stripped).
	PathPrefix string
}

func New(config Config) *App {
	funcs := template.FuncMap{
		"route": func(pathParts ...string) string {
			return config.PathPrefix + "/" + strings.Join(pathParts, "/")
		},
		"join": strings.Join,
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return humanize.Time(t)
		},
		"toJSON": func(v any) (template.JS, error) {
			b, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return template.JS(b), nil
		},
		"prettyJSON": func(b []byte) (template.JS, error) {
			var buf bytes.Buffer
			if err := json.Indent(&buf, b, "", "  "); err != nil {
				return "", err
			}
			return template.JS(buf.String()), nil
		},
	}

	return &App{
		client:  config.Client,
		logger:  config.Log,
		index:   template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "index.html")),
		queues:  template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "queues.html")),
		tasks:   template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "tasks.html")),
		task:    template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "task.html", "task_runs.html")),
		flows:   template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "flows.html")),
		flow:    template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "flow.html", "flow_runs.html")),
		workers: template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "workers.html")),
	}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /queues", a.handleQueues)
	mux.HandleFunc("GET /queues/table", a.handleQueuesTable)
	mux.HandleFunc("GET /queue/create-form", a.handleCreateQueueForm)
	mux.HandleFunc("POST /queue/create", a.handleCreateQueue)
	mux.HandleFunc("GET /queue/send-form", a.handleSendMessageForm)
	mux.HandleFunc("POST /queue/send", a.handleSendMessage)
	mux.HandleFunc("GET /tasks", a.handleTasks)
	mux.HandleFunc("GET /task/{task_name}", a.handleTask)
	mux.HandleFunc("GET /task/{task_name}/runs", a.handleTaskRuns)
	mux.HandleFunc("GET /task/{task_name}/form", a.handleTaskStartRunForm)
	mux.HandleFunc("POST /task/{task_name}/run", a.handleStartTaskRun)
	mux.HandleFunc("GET /flows", a.handleFlows)
	mux.HandleFunc("GET /flow/{flow_name}", a.handleFlow)
	mux.HandleFunc("GET /flow/{flow_name}/runs", a.handleFlowRuns)
	mux.HandleFunc("GET /flow/{flow_name}/form", a.handleFlowStartRunForm)
	mux.HandleFunc("POST /flow/{flow_name}/run", a.handleStartFlowRun)
	mux.HandleFunc("GET /workers", a.handleWorkers)
	mux.HandleFunc("GET /workers/table", a.handleWorkersTable)
	mux.HandleFunc("GET /dark.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css")
		css, _ := templatesFS.ReadFile("dark.css")
		w.Write(css)
	})

	return mux
}

func (a *App) render(w http.ResponseWriter, r *http.Request, t *template.Template, data any) {
	var tmpl string
	if r.Header.Get("HX-Request") == "true" {
		tmpl = "content"
	} else {
		tmpl = "page"
	}

	if err := t.ExecuteTemplate(w, tmpl, data); err != nil {
		// Headers may already be written, so just log the error
		a.logger.Error("template execution error", "error", err)
		return
	}
}

func (a *App) handleError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		a.logger.Warn("resource not found", "path", r.URL.Path)
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	a.logger.Error("handler error", "error", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, a.index, nil)
}

func (a *App) handleQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := a.client.ListQueues(r.Context())
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.render(w, r, a.queues, struct {
		Queues []*catbird.QueueInfo
	}{
		Queues: queues,
	})
}

func (a *App) handleQueuesTable(w http.ResponseWriter, r *http.Request) {
	queues, err := a.client.ListQueues(r.Context())
	if err != nil {
		a.logger.Error("failed to list queues", "error", err)
		return
	}

	if err := a.queues.ExecuteTemplate(w, "queues_table", struct {
		Queues []*catbird.QueueInfo
	}{
		Queues: queues,
	}); err != nil {
		a.logger.Error("template execution error", "error", err)
	}
}

func (a *App) handleSendMessageForm(w http.ResponseWriter, r *http.Request) {
	queues, err := a.client.ListQueues(r.Context())
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	if err := a.queues.ExecuteTemplate(w, "send_message_form", struct {
		Queues []*catbird.QueueInfo
	}{
		Queues: queues,
	}); err != nil {
		a.handleError(w, r, err)
	}
}

func (a *App) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.queues.ExecuteTemplate(w, "send_message_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	queue := r.FormValue("queue")
	topic := r.FormValue("topic")
	payload := r.FormValue("payload")

	if payload == "" {
		payload = "{}"
	}

	opts := catbird.SendOpts{}
	if topic != "" {
		opts.Topic = topic
	}

	err := a.client.SendWithOpts(r.Context(), queue, json.RawMessage(payload), opts)
	if err != nil {
		a.queues.ExecuteTemplate(w, "send_message_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	a.queues.ExecuteTemplate(w, "send_message_success", nil)
}

func (a *App) handleCreateQueueForm(w http.ResponseWriter, r *http.Request) {
	if err := a.queues.ExecuteTemplate(w, "create_queue_form", nil); err != nil {
		a.handleError(w, r, err)
	}
}

func (a *App) handleCreateQueue(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.queues.ExecuteTemplate(w, "create_queue_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	name := r.FormValue("name")
	if name == "" {
		a.queues.ExecuteTemplate(w, "create_queue_error", struct {
			Error string
		}{
			Error: "queue name is required",
		})
		return
	}

	err := a.client.CreateQueue(r.Context(), name)
	if err != nil {
		a.queues.ExecuteTemplate(w, "create_queue_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	a.queues.ExecuteTemplate(w, "create_queue_success", nil)
}

func (a *App) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := a.client.ListTasks(r.Context())
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.render(w, r, a.tasks, struct {
		Tasks []*catbird.TaskInfo
	}{
		Tasks: tasks,
	})
}

func (a *App) handleTask(w http.ResponseWriter, r *http.Request) {
	taskName := r.PathValue("task_name")

	task, err := a.client.GetTask(r.Context(), taskName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	taskRuns, err := a.client.ListTaskRuns(r.Context(), taskName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.render(w, r, a.task, struct {
		Task     *catbird.TaskInfo
		TaskRuns []*catbird.RunInfo
	}{
		Task:     task,
		TaskRuns: taskRuns,
	})
}

func (a *App) handleFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := a.client.ListFlows(r.Context())
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.render(w, r, a.flows, struct {
		Flows []*catbird.FlowInfo
	}{
		Flows: flows,
	})
}

func (a *App) handleFlow(w http.ResponseWriter, r *http.Request) {
	flowName := r.PathValue("flow_name")

	flow, err := a.client.GetFlow(r.Context(), flowName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	flowRuns, err := a.client.ListFlowRuns(r.Context(), flowName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	// Fetch step runs for each flow run
	stepRunsByFlowRun := make(map[int64][]*catbird.StepRunInfo)
	for _, flowRun := range flowRuns {
		stepRuns, err := a.client.GetFlowRunSteps(r.Context(), flowName, flowRun.ID)
		if err != nil {
			a.logger.Warn("Failed to fetch step runs", "flow_run_id", flowRun.ID, "error", err)
			continue
		}
		stepRunsByFlowRun[flowRun.ID] = stepRuns
	}

	a.render(w, r, a.flow, struct {
		Flow              *catbird.FlowInfo
		FlowRuns          []*catbird.RunInfo
		StepRunsByFlowRun map[int64][]*catbird.StepRunInfo
	}{
		Flow:              flow,
		FlowRuns:          flowRuns,
		StepRunsByFlowRun: stepRunsByFlowRun,
	})
}

func (a *App) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.client.ListWorkers(r.Context())
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.render(w, r, a.workers, struct {
		Workers []*catbird.WorkerInfo
	}{
		Workers: workers,
	})
}

func (a *App) handleWorkersTable(w http.ResponseWriter, r *http.Request) {
	workers, err := a.client.ListWorkers(r.Context())
	if err != nil {
		a.logger.Error("failed to list workers", "error", err)
		return
	}

	if err := a.workers.ExecuteTemplate(w, "workers_table", struct {
		Workers []*catbird.WorkerInfo
	}{
		Workers: workers,
	}); err != nil {
		a.logger.Error("template execution error", "error", err)
	}
}

func (a *App) handleTaskRuns(w http.ResponseWriter, r *http.Request) {
	taskName := r.PathValue("task_name")

	task, err := a.client.GetTask(r.Context(), taskName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	taskRuns, err := a.client.ListTaskRuns(r.Context(), taskName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	// Render just the task runs table partial
	if err := a.task.ExecuteTemplate(w, "task_runs_table", struct {
		Task     *catbird.TaskInfo
		TaskRuns []*catbird.RunInfo
	}{
		Task:     task,
		TaskRuns: taskRuns,
	}); err != nil {
		a.handleError(w, r, err)
	}
}

func (a *App) handleTaskStartRunForm(w http.ResponseWriter, r *http.Request) {
	taskName := r.PathValue("task_name")

	task, err := a.client.GetTask(r.Context(), taskName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	if err := a.task.ExecuteTemplate(w, "start_task_run_form", struct {
		Task *catbird.TaskInfo
	}{
		Task: task,
	}); err != nil {
		a.handleError(w, r, err)
	}
}

func (a *App) handleStartTaskRun(w http.ResponseWriter, r *http.Request) {
	taskName := r.PathValue("task_name")

	if err := r.ParseForm(); err != nil {
		a.task.ExecuteTemplate(w, "start_task_run_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	input := r.FormValue("input")
	if input == "" {
		input = "{}"
	}

	_, err := a.client.RunTask(r.Context(), taskName, json.RawMessage(input))
	if err != nil {
		a.task.ExecuteTemplate(w, "start_task_run_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	// Return success message - table will auto-refresh
	a.task.ExecuteTemplate(w, "start_task_run_success", nil)
}

func (a *App) handleFlowRuns(w http.ResponseWriter, r *http.Request) {
	flowName := r.PathValue("flow_name")

	flow, err := a.client.GetFlow(r.Context(), flowName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	flowRuns, err := a.client.ListFlowRuns(r.Context(), flowName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	// Fetch step runs for each flow run
	stepRunsByFlowRun := make(map[int64][]*catbird.StepRunInfo)
	for _, flowRun := range flowRuns {
		stepRuns, err := a.client.GetFlowRunSteps(r.Context(), flowName, flowRun.ID)
		if err != nil {
			a.logger.Warn("Failed to fetch step runs", "flow_run_id", flowRun.ID, "error", err)
			continue
		}
		stepRunsByFlowRun[flowRun.ID] = stepRuns
	}

	// Render just the flow runs table partial
	if err := a.flow.ExecuteTemplate(w, "flow_runs_table", struct {
		Flow              *catbird.FlowInfo
		FlowRuns          []*catbird.RunInfo
		StepRunsByFlowRun map[int64][]*catbird.StepRunInfo
	}{
		Flow:              flow,
		FlowRuns:          flowRuns,
		StepRunsByFlowRun: stepRunsByFlowRun,
	}); err != nil {
		a.handleError(w, r, err)
	}
}

func (a *App) handleFlowStartRunForm(w http.ResponseWriter, r *http.Request) {
	flowName := r.PathValue("flow_name")

	flow, err := a.client.GetFlow(r.Context(), flowName)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	if err := a.flow.ExecuteTemplate(w, "start_flow_run_form", struct {
		Flow *catbird.FlowInfo
	}{
		Flow: flow,
	}); err != nil {
		a.handleError(w, r, err)
	}
}

func (a *App) handleStartFlowRun(w http.ResponseWriter, r *http.Request) {
	flowName := r.PathValue("flow_name")

	if err := r.ParseForm(); err != nil {
		a.flow.ExecuteTemplate(w, "start_flow_run_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	input := r.FormValue("input")
	if input == "" {
		input = "{}"
	}

	_, err := a.client.RunFlow(r.Context(), flowName, json.RawMessage(input))
	if err != nil {
		a.flow.ExecuteTemplate(w, "start_flow_run_error", struct {
			Error string
		}{
			Error: err.Error(),
		})
		return
	}

	// Return success message - table will auto-refresh
	a.flow.ExecuteTemplate(w, "start_flow_run_success", nil)
}
