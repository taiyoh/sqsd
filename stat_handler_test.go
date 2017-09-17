package sqsd

import (
	"encoding/json"
	"strconv"
	"net/http"
	"testing"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
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
	var r SQSStatCurrentJobsResponse
	if err := json.Unmarshal(w.ResBytes, &r); err != nil {
		t.Error("json unmarshal error", err)
	}
	if len(r.CurrentJobs) != 1 {
		t.Error("current_jobs count invalid")
	}
	if r.CurrentJobs[0].ID != "1" {
		t.Error("job id invalid")
	}
}

func TestNewStatHandler(t *testing.T) {
	h := NewStatHandler(&SQSJobTracker{})
	if h == nil {
		t.Error("stat handler not loaded.")
	}
}

func TestWorkerCurrentSummaryAndJobsHandler(t *testing.T) {
	tr := NewJobTracker(5)
	h := NewStatHandler(tr)

	for i := 1; i <= tr.MaxProcessCount; i++ {
		j := &SQSJob{
			Msg: &sqs.Message{
				MessageId: aws.String("id:" + strconv.Itoa(i)),
				Body:      aws.String(`foobar`),
			},
		}
		tr.Add(j)
	}

	summaryController := h.WorkerCurrentSummaryHandler()
	req := &http.Request{}
	req.Method = "POST"	

	t.Run("invalid Method for summary", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		summaryController(w, req)

		if w.StatusCode == http.StatusOK {
			t.Error("response error found")
		}
	})

	t.Run("valid Method for summary", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		req.Method = "GET"
		summaryController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response error found")
		}

		var r SQSStatCurrentSummaryResponse
		if err := json.Unmarshal(w.ResBytes, &r); err != nil {
			t.Error("json unmarshal error", err)
		}

		if r.JobsCount != tr.MaxProcessCount {
			t.Error("job summaries invalid")
		}

		if r.RestCount != 0 {
			t.Error("rest count invalid")
		}
	})

	jobsController := h.WorkerCurrentJobsHandler()
	req.Method = "POST"

	t.Run("invalid Method for jobs", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		jobsController(w, req)

		if w.StatusCode == http.StatusOK {
			t.Error("response error found")
		}
	})

	t.Run("valid Method for jobs", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		req.Method = "GET"
		jobsController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response error found")
		}

		var r SQSStatCurrentJobsResponse
		if err := json.Unmarshal(w.ResBytes, &r); err != nil {
			t.Error("json unmarshal error", err)
		}

		if len(r.CurrentJobs) != tr.MaxProcessCount {
			t.Error("current_jobs count invalid")
		}

		for _, summary := range r.CurrentJobs {
			if _, exists := tr.CurrentWorkings[summary.ID]; !exists {
				t.Errorf("job summary not registered: %s\n", summary.ID)
			}
		}
	})
}

func TestWorkerPauseAndResumeHandler(t *testing.T) {
	tr := NewJobTracker(5)
	h := NewStatHandler(tr)

	pauseController := h.WorkerPauseHandler()

	req := &http.Request{}

	req.Method = "GET"	
	t.Run("pause failed", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		pauseController(w, req)

		if w.StatusCode != http.StatusMethodNotAllowed {
			t.Error("response code invalid")
		}

		if !tr.IsWorking() {
			t.Error("IsWorking changed")
		}
	})

	req.Method = "POST"
	t.Run("pause success", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		pauseController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response code invalid")
		}

		if tr.IsWorking() {
			t.Error("IsWorking not changed")
		}
	})

	resumeController := h.WorkerResumeHandler()

	req.Method = "GET"	
	t.Run("resume failed", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		resumeController(w, req)

		if w.StatusCode != http.StatusMethodNotAllowed {
			t.Error("response code invalid")
		}

		if tr.IsWorking() {
			t.Error("IsWorking changed")
		}
	})

	req.Method = "POST"
	t.Run("resume success", func(t *testing.T) {
		w := NewSQSMockResponseWriter()
		resumeController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response code invalid")
		}

		if !tr.IsWorking() {
			t.Error("IsWorking not changed")
		}
	})

}