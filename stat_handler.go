package sqsd

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/fukata/golang-stats-api-handler"
)

// StatHandler provides monitoring processing queues and process resource
type StatHandler struct {
	Tracker *QueueTracker
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

// StatCurrentSummaryResponse provides response object for /worker/current request
type StatCurrentSummaryResponse struct {
	JobsCount int  `json:"jobs_count"`
	IsWorking bool `json:"is_working"`
}

// JSONString returns json string building from itself
func (r *StatCurrentSummaryResponse) JSONString() string {
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

// WorkerCurrentSummaryHandler returns http.HandlerFunc implementation for /worker/current request
func (h *StatHandler) WorkerCurrentSummaryHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, "GET") {
			return
		}
		jobsCount := len(h.Tracker.CurrentSummaries())
		renderJSON(w, &StatCurrentSummaryResponse{
			JobsCount: jobsCount,
			IsWorking: h.Tracker.IsWorking(),
		})
	}
}

// WorkerCurrentJobsHandler returns http.HandlerFunc implementation for /worker/current/jobs request
func (h *StatHandler) WorkerCurrentJobsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, "GET") {
			return
		}
		renderJSON(w, &StatCurrentJobsResponse{
			CurrentJobs: h.Tracker.CurrentSummaries(),
		})
	}
}

// WorkerPauseHandler returns http.HandlerFunc implementation for /worker/pause request
func (h *StatHandler) WorkerPauseHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, "POST") {
			return
		}
		h.Tracker.Pause()
		renderJSON(w, &StatSuccessResponse{
			Success: true,
		})
	}
}

// WorkerResumeHandler returns http.HandlerFunc implementation for /worker/resume request
func (h *StatHandler) WorkerResumeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !reqMethodValidate(w, r, "POST") {
			return
		}
		h.Tracker.Resume()
		renderJSON(w, &StatSuccessResponse{
			Success: true,
		})
	}
}

// BuildServeMux returns http.ServeMux object with registered endpoints
func (h *StatHandler) BuildServeMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats", stats_api.Handler)
	mux.HandleFunc("/worker/current", h.WorkerCurrentSummaryHandler())
	mux.HandleFunc("/worker/current/jobs", h.WorkerCurrentJobsHandler())
	mux.HandleFunc("/worker/pause", h.WorkerPauseHandler())
	mux.HandleFunc("/worker/resume", h.WorkerResumeHandler())

	return mux
}
