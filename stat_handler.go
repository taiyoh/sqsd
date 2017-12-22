package sqsd

import (
	"encoding/json"
	"fmt"
	"github.com/fukata/golang-stats-api-handler"
	"net/http"
)

type StatHandler struct {
	Tracker *JobTracker
}

type StatResponseIFace interface {
	JSONString() string
}

type StatCurrentJobsResponse struct {
	CurrentJobs []*JobSummary `json:"current_jobs"`
}

func (r *StatCurrentJobsResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

type StatSuccessResponse struct {
	Success bool `json:"success"`
}

func (r *StatSuccessResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

type StatCurrentSummaryResponse struct {
	JobsCount int  `json:"jobs_count"`
	IsWorking bool `json:"is_working"`
}

func (r *StatCurrentSummaryResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

func ReqMethodValidate(w http.ResponseWriter, r *http.Request, m string) bool {
	if r.Method == m {
		return true
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusMethodNotAllowed)
	fmt.Fprint(w, "Method Not Allowed")
	return false
}

func RenderJSON(w http.ResponseWriter, res StatResponseIFace) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, res.JSONString())
}

func (h *StatHandler) WorkerCurrentSummaryHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		jobsCount := len(h.Tracker.CurrentSummaries())
		RenderJSON(w, &StatCurrentSummaryResponse{
			JobsCount: jobsCount,
			IsWorking: h.Tracker.IsWorking(),
		})
	}
}

func (h *StatHandler) WorkerCurrentJobsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		RenderJSON(w, &StatCurrentJobsResponse{
			CurrentJobs: h.Tracker.CurrentSummaries(),
		})
	}
}

func (h *StatHandler) WorkerPauseHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "POST") {
			return
		}
		h.Tracker.Pause()
		RenderJSON(w, &StatSuccessResponse{
			Success: true,
		})
	}
}

func (h *StatHandler) WorkerResumeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "POST") {
			return
		}
		h.Tracker.Resume()
		RenderJSON(w, &StatSuccessResponse{
			Success: true,
		})
	}
}

func (h *StatHandler) BuildServeMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats", stats_api.Handler)
	mux.HandleFunc("/worker/current", h.WorkerCurrentSummaryHandler())
	mux.HandleFunc("/worker/current/jobs", h.WorkerCurrentJobsHandler())
	mux.HandleFunc("/worker/pause", h.WorkerPauseHandler())
	mux.HandleFunc("/worker/resume", h.WorkerResumeHandler())

	return mux
}
