package sqsd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	stats_api "github.com/fukata/golang-stats-api-handler"
	"golang.org/x/sync/errgroup"
)

// StatHandler provides monitoring processing queues and process resource
type StatHandler struct {
	tracker *QueueTracker
}

// NewStatHandler returns new StatHandler object.
func NewStatHandler(tr *QueueTracker) *StatHandler {
	return &StatHandler{tracker: tr}
}

// StatResponseIFace is interface for JSON response dumper
type StatResponseIFace interface {
	JSONString() string
}

// StatCurrentJobsResponse provides response object for /worker/current/jobs request
type StatCurrentJobsResponse struct {
	CurrentJobs []QueueSummary `json:"current_jobs"`
}

// JSONString returns json string building from itself
func (r *StatCurrentJobsResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

// StatSuccessResponse provides response object for /worker/(pause|resume) request
type StatSuccessResponse struct {
	Success bool `json:"success"`
}

// JSONString returns json string building from itself
func (r *StatSuccessResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

// StatWorkerStatsResponse provides response object for /worker/stats request
type StatWorkerStatsResponse struct {
	IsWorking      bool `json:"is_working"`
	TotalHandled   int  `json:"total_handled"`
	TotalSucceeded int  `json:"total_succeeded"`
	TotalFailed    int  `json:"total_failed"`
	MaxWorker      int  `json:"max_worker"`
	BusyWorker     int  `json:"busy_worker"`
	IdleWorker     int  `json:"idle_worker"`
}

// JSONString returns json string building from itself
func (r *StatWorkerStatsResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

func reqMethodValidate(w http.ResponseWriter, r *http.Request, m string) bool {
	if r.Method == m {
		return true
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusMethodNotAllowed)
	fmt.Fprint(w, "Method Not Allowed")
	return false
}

func renderJSON(w http.ResponseWriter, res StatResponseIFace) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, res.JSONString())
}

// WorkerStatsHandler returns http.HandlerFunc implementation for /worker/current request
func (h *StatHandler) WorkerStatsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, http.MethodGet) {
			return
		}
		busy := len(h.tracker.CurrentSummaries())
		renderJSON(w, &StatWorkerStatsResponse{
			IsWorking:      h.tracker.IsWorking(),
			TotalHandled:   int(h.tracker.scoreBoard.TotalHandled()),
			TotalSucceeded: int(h.tracker.scoreBoard.TotalSucceeded),
			TotalFailed:    int(h.tracker.scoreBoard.TotalFailed),
			MaxWorker:      h.tracker.scoreBoard.MaxWorker,
			BusyWorker:     busy,
			IdleWorker:     h.tracker.scoreBoard.MaxWorker - busy,
		})
	}
}

// WorkerCurrentJobsHandler returns http.HandlerFunc implementation for /worker/current/jobs request
func (h *StatHandler) WorkerCurrentJobsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, http.MethodGet) {
			return
		}
		renderJSON(w, &StatCurrentJobsResponse{
			CurrentJobs: h.tracker.CurrentSummaries(),
		})
	}
}

// WorkerPauseHandler returns http.HandlerFunc implementation for /worker/pause request
func (h *StatHandler) WorkerPauseHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, http.MethodPost) {
			return
		}
		h.tracker.Pause()
		renderJSON(w, &StatSuccessResponse{
			Success: true,
		})
	}
}

// WorkerResumeHandler returns http.HandlerFunc implementation for /worker/resume request
func (h *StatHandler) WorkerResumeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, http.MethodPost) {
			return
		}
		h.tracker.Resume()
		renderJSON(w, &StatSuccessResponse{
			Success: true,
		})
	}
}

// BuildServeMux returns http.ServeMux object with registered endpoints
func (h *StatHandler) BuildServeMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats", stats_api.Handler)
	mux.HandleFunc("/worker/stats", h.WorkerStatsHandler())
	mux.HandleFunc("/worker/current/jobs", h.WorkerCurrentJobsHandler())
	mux.HandleFunc("/worker/pause", h.WorkerPauseHandler())
	mux.HandleFunc("/worker/resume", h.WorkerResumeHandler())

	return mux
}

// RunStatServer provides running stat server.
func RunStatServer(ctx context.Context, tr *QueueTracker, port int) error {
	handler := NewStatHandler(tr)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler.BuildServeMux(),
	}

	logger := tr.logger

	logger.Info("stat server start.")
	defer logger.Info("stat server stop.")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(ctx)
	})

	if err := eg.Wait(); err != nil {
		logger.Infof("stat server err: %v", err)
		return err
	}

	return nil
}
