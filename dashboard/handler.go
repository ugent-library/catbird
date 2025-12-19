package dashboard

import (
	"embed"
	"html/template"
	"log/slog"
	"net/http"

	"github.com/ugent-library/catbird"
)

//go:embed *.html
var templatesFS embed.FS

type App struct {
	client    *catbird.Client
	logger    *slog.Logger
	templates *template.Template
}

type Config struct {
	Client     *catbird.Client
	Logger     *slog.Logger
	PathPrefix string
}

func New(config Config) *App {
	t := template.Must(template.New("").Funcs(template.FuncMap{
		"route": func(path string) string {
			return config.PathPrefix + path
		},
	}).ParseFS(templatesFS, "*.html"))

	return &App{
		client:    config.Client,
		logger:    config.Logger,
		templates: t,
	}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /queues", a.handleQueues)
	mux.HandleFunc("GET /workers", a.handleWorkers)

	return mux
}

func (a *App) render(w http.ResponseWriter, r *http.Request, tmpl string, data any) {
	if err := a.templates.ExecuteTemplate(w, tmpl+".html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) handeError(w http.ResponseWriter, r *http.Request, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, "index", nil)
}

func (a *App) handleQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := a.client.ListQueues(r.Context())
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, "queues", struct {
		Queues []catbird.QueueInfo
	}{
		Queues: queues,
	})
}

func (a *App) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.client.ListWorkers(r.Context())
	if err != nil {
		a.handeError(w, r, err)
		return
	}

	a.render(w, r, "workers", struct {
		Workers []catbird.WorkerInfo
	}{
		Workers: workers,
	})
}
