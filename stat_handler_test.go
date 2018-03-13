package sqsd

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestReqMethodValidate(t *testing.T) {
	req := &http.Request{}
	w := NewMockResponseWriter()
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
	w := NewMockResponseWriter()
	RenderJSON(w, &StatSuccessResponse{
		Success: true,
	})
	if w.ResponseString() != `{"success":true}` {
		t.Errorf("response body failed: %s\n", w.ResponseString())
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Error("response type failed")
	}
	w.ResBytes = []byte{} // clear
	RenderJSON(w, &StatCurrentJobsResponse{
		CurrentJobs: []*QueueSummary{
			&QueueSummary{ID: "1", Payload: "p1", ReceivedAt: 10},
		},
	})
	var r StatCurrentJobsResponse
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

func TestWorkerCurrentSummaryAndJobsHandler(t *testing.T) {
	tr := NewJobTracker(5)
	h := &StatHandler{tr}

	for i := 1; i <= 5; i++ {
		q := &Queue{
			Msg: &sqs.Message{
				MessageId: aws.String("id:" + strconv.Itoa(i)),
				Body:      aws.String(`foobar`),
			},
		}
		tr.Register(q)
	}

	summaryController := h.WorkerCurrentSummaryHandler()
	req := &http.Request{}
	req.Method = "POST"

	t.Run("invalid Method for summary", func(t *testing.T) {
		w := NewMockResponseWriter()
		summaryController(w, req)

		if w.StatusCode != http.StatusMethodNotAllowed {
			t.Error("response error found")
		}
	})

	t.Run("valid Method for summary", func(t *testing.T) {
		w := NewMockResponseWriter()
		req.Method = "GET"
		summaryController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response error found")
		}

		var r StatCurrentSummaryResponse
		if err := json.Unmarshal(w.ResBytes, &r); err != nil {
			t.Error("json unmarshal error", err)
		}

		if r.JobsCount != 5 {
			t.Error("job summaries invalid")
		}

		if !r.IsWorking {
			t.Error("is_working invalid")
		}
	})

	jobsController := h.WorkerCurrentJobsHandler()
	req.Method = "POST"

	t.Run("invalid Method for jobs", func(t *testing.T) {
		w := NewMockResponseWriter()
		jobsController(w, req)

		if w.StatusCode == http.StatusOK {
			t.Error("response error found")
		}
	})

	t.Run("valid Method for jobs", func(t *testing.T) {
		w := NewMockResponseWriter()
		req.Method = "GET"
		jobsController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response error found")
		}

		var r StatCurrentJobsResponse
		if err := json.Unmarshal(w.ResBytes, &r); err != nil {
			t.Error("json unmarshal error", err)
		}

		if len(r.CurrentJobs) != 5 {
			t.Error("current_jobs count invalid")
		}

		for _, summary := range r.CurrentJobs {
			if _, exists := tr.CurrentWorkings.Load(summary.ID); !exists {
				t.Errorf("job summary not registered: %s\n", summary.ID)
			}
		}
	})
}

func TestWorkerPauseAndResumeHandler(t *testing.T) {
	tr := NewJobTracker(5)
	h := &StatHandler{tr}

	pauseController := h.WorkerPauseHandler()

	req := &http.Request{}

	req.Method = "GET"
	t.Run("pause failed", func(t *testing.T) {
		w := NewMockResponseWriter()
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
		w := NewMockResponseWriter()
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
		w := NewMockResponseWriter()
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
		w := NewMockResponseWriter()
		resumeController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response code invalid")
		}

		if !tr.IsWorking() {
			t.Error("IsWorking not changed")
		}
	})

}
