package sqsd

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type SQSStatHandler struct {
	Tracker *SQSJobTracker
}

type SQSStatResponseIFace interface {
	JSONString() string
}

type SQSStatCurrentJobsResponse struct {
	CurrentJobs []*SQSJobSummary `json:"current_jobs"`
}

func (r *SQSStatCurrentJobsResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

type SQSStatSuccessResponse struct {
	Success bool `json:"success"`
}

func (r *SQSStatSuccessResponse) JSONString() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

type SQSStatCurrentSummaryResponse struct {
	JobsCount int `json:"jobs_count"`
	RestCount int `json:"rest_count"`
}

func (r *SQSStatCurrentSummaryResponse) JSONString() string {
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

func RenderJSON(w http.ResponseWriter, res SQSStatResponseIFace) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, res.JSONString())
}

func (h *SQSStatHandler) WorkerCurrentSummaryHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		jobsCount := len(h.Tracker.CurrentSummaries())
		RenderJSON(w, &SQSStatCurrentSummaryResponse{
			JobsCount: jobsCount,
			RestCount: h.Tracker.MaxProcessCount - jobsCount,
		})
	}
}

func (h *SQSStatHandler) WorkerCurrentJobsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		RenderJSON(w, &SQSStatCurrentJobsResponse{
			CurrentJobs: h.Tracker.CurrentSummaries(),
		})
	}
}

func (h *SQSStatHandler) WorkerPauseHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "POST") {
			return
		}
		h.Tracker.Pause()
		RenderJSON(w, &SQSStatSuccessResponse{
			Success: true,
		})
	}
}

func (h *SQSStatHandler) WorkerResumeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "POST") {
			return
		}
		h.Tracker.Resume()
		RenderJSON(w, &SQSStatSuccessResponse{
			Success: true,
		})
	}
}

func NewStatHandler(tracker *SQSJobTracker) *SQSStatHandler {
	return &SQSStatHandler{
		Tracker: tracker,
	}
}
