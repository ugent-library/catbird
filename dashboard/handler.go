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
	client  *catbird.Client
	logger  *slog.Logger
	index   *template.Template
	queues  *template.Template
	workers *template.Template
}

type Config struct {
	Client     *catbird.Client
	Log        *slog.Logger
	PathPrefix string
}

func New(config Config) *App {
	funcs := template.FuncMap{
		"route": func(path string) string {
			return config.PathPrefix + path
		},
	}

	return &App{
		client:  config.Client,
		logger:  config.Log,
		index:   template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "index.html")),
		queues:  template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "queues.html")),
		workers: template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, "page.html", "workers.html")),
	}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /queues", a.handleQueues)
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
