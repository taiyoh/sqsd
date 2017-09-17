package sqsd

import (
	"net/http"
	"testing"
)

func TestReqMethodValidate(t *testing.T) {
	req := &http.Request{}
	w := NewSQSMockResponseWriter()
	req.Method = "GET"
	if !ReqMethodValidate(w, req, "GET") {
		t.Error("validation failed")
	}
	if len(w.ResBytes) > 0 {
		t.Error("response inserted")
	}
	req.Method = "POST"
	if ReqMethodValidate(w, req, "GET") {
		t.Error("validation failed")
	}
	if w.ResponseString() != "Method Not Allowed" {
		t.Error("response body failed")
	}
	if w.StatusCode != http.StatusMethodNotAllowed {
		t.Error("response code failed")
	}
}

func TestRenderJSON(t *testing.T) {
	w := NewSQSMockResponseWriter()
	RenderJSON(w, &SQSStatSuccessResponse{
		Success: true,
	})
	if w.ResponseString() != `{"success":true}` {
		t.Errorf("response body failed: %s\n", w.ResponseString())
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Error("response type failed")
	}
	w.ResBytes = []byte{} // clear
	RenderJSON(w, &SQSStatCurrentJobsResponse{
		CurrentJobs: []*SQSJobSummary{
			&SQSJobSummary{ID:"1",Payload:"p1",StartAt:10},
		},
	})
	if len(w.ResBytes) != len(`{"current_jobs":[{"id":"1","payload":"p1","start_at":10}]}`) {
		t.Errorf("response body failed: %s", w.ResponseString())
	}
}

func TestNewStatHandler(t *testing.T) {
	h := NewStatHandler(&SQSJobTracker{})
	if h == nil {
		t.Error("stat handler not loaded.")
	}
}
