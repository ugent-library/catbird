package dashboard

import (
	"bytes"
	"embed"
	"encoding/json"
	"html/template"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ugent-library/catbird"
)

//go:embed *.html
var templatesFS embed.FS

type App struct {
	client   *catbird.Client
	logger   *slog.Logger
	index    *template.Template
	queues   *template.Template
	tasks    *template.Template
	taskRuns *template.Template
	flows    *template.Template
	flowRuns *template.Template
	workers  *template.Template
}

type Config struct {
	Client     *catbird.Client
	Log        *slog.Logger
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
			return t.Format(time.RFC3339)
		},
		"prettyJSON": func(b []byte) (string, error) {
			var buf bytes.Buffer
			if err := json.Indent(&buf, b, "", "  "); err != nil {
				return "", err
			}
			return buf.String(), nil
		},
	}

	return &App{
		client:   config.Client,
		logger:   config.Log,
		index:    template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "index.html")),
		queues:   template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "queues.html")),
		tasks:    template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "tasks.html")),
		taskRuns: template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "task_runs.html")),
		flows:    template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "flows.html")),
		flowRuns: template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "flow_runs.html")),
		workers:  template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "workers.html")),
	}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /queues", a.handleQueues)
	mux.HandleFunc("GET /tasks", a.handleTasks)
	mux.HandleFunc("GET /task/{task_name}/runs", a.handleTaskRuns)
	mux.HandleFunc("GET /flows", a.handleFlows)
	mux.HandleFunc("GET /flow/{flow_name}/runs", a.handleFlowRuns)
	mux.HandleFunc("GET /workers", a.handleWorkers)

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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) handeError(w http.ResponseWriter, r *http.Request, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, a.index, nil)
}

func (a *App) handleQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := a.client.ListQueues(r.Context())
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, a.queues, struct {
		Queues []catbird.QueueInfo
	}{
		Queues: queues,
	})
}

func (a *App) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := a.client.ListTasks(r.Context())
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, a.tasks, struct {
		Tasks []catbird.TaskInfo
	}{
		Tasks: tasks,
	})
}

func (a *App) handleTaskRuns(w http.ResponseWriter, r *http.Request) {
	taskName := r.PathValue("task_name")

	taskRuns, err := a.client.ListTaskRuns(r.Context(), taskName)
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, a.taskRuns, struct {
		TaskName string
		TaskRuns []*catbird.TaskRunInfo
	}{
		TaskName: taskName,
		TaskRuns: taskRuns,
	})
}

func (a *App) handleFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := a.client.ListFlows(r.Context())
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, a.flows, struct {
		Flows []*catbird.FlowInfo
	}{
		Flows: flows,
	})
}

func (a *App) handleFlowRuns(w http.ResponseWriter, r *http.Request) {
	flowName := r.PathValue("flow_name")

	flowRuns, err := a.client.ListFlowRuns(r.Context(), flowName)
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, a.flowRuns, struct {
		FlowName string
		FlowRuns []*catbird.FlowRunInfo
	}{
		FlowName: flowName,
		FlowRuns: flowRuns,
	})
}

func (a *App) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.client.ListWorkers(r.Context())
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, a.workers, struct {
		Workers []catbird.WorkerInfo
	}{
		Workers: workers,
	})
}
