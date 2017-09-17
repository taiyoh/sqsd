package sqsd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type SQSStatHandler struct {
	Tracker *SQSJobTracker
}

type SQSStatResponseIFace interface{}

type SQSStatCurrentListResponse struct {
	SQSStatResponseIFace
	CurrentList []*SQSJobSummary `json:"current_list"`
}

type SQSStatCurrentSizeResponse struct {
	SQSStatResponseIFace
	Size int `json:"size"`
}

type SQSStatSuccessResponse struct {
	SQSStatResponseIFace
	Success bool `json:"success"`
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
	buf, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, bytes.NewBuffer(buf).String())
}

func (h *SQSStatHandler) WorkerCurrentListHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		RenderJSON(w, &SQSStatCurrentListResponse{
			CurrentList: h.Tracker.CurrentSummaries(),
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
