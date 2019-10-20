package sqsd_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/taiyoh/sqsd"
)

func TestWorkerStatsAndJobsHandler(t *testing.T) {
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))
	h := sqsd.NewStatHandler(tr)

	for i := 1; i <= 5; i++ {
		tr.Register(sqsd.Queue{
			ID:      fmt.Sprintf("id:%d", i),
			Payload: "foobar",
			Receipt: fmt.Sprintf("reciept:%d", i),
		})
	}

	workerStatsController := h.WorkerStatsHandler()
	req := &http.Request{}
	req.Method = "POST"

	t.Run("invalid Method for summary", func(t *testing.T) {
		w := NewMockResponseWriter()
		workerStatsController(w, req)

		if w.StatusCode != http.StatusMethodNotAllowed {
			t.Error("response error found")
		}
	})

	t.Run("valid Method for summary", func(t *testing.T) {
		w := NewMockResponseWriter()
		req.Method = "GET"
		workerStatsController(w, req)

		if w.StatusCode != http.StatusOK {
			t.Error("response error found")
		}

		var r sqsd.StatWorkerStatsResponse
		if err := json.Unmarshal(w.ResBytes, &r); err != nil {
			t.Error("json unmarshal error", err)
		}

		if len(tr.CurrentSummaries()) != 5 {
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

		var r sqsd.StatCurrentJobsResponse
		if err := json.Unmarshal(w.ResBytes, &r); err != nil {
			t.Error("json unmarshal error", err)
		}

		if len(r.CurrentJobs) != 5 {
			t.Error("current_jobs count invalid")
		}

		for _, summary := range r.CurrentJobs {
			if _, exists := tr.Find(summary.ID); !exists {
				t.Errorf("job summary not registered: %s\n", summary.ID)
			}
		}
	})
}

func TestWorkerPauseAndResumeHandler(t *testing.T) {
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))
	h := sqsd.NewStatHandler(tr)

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

func TestStatHandlerServeMux(t *testing.T) {
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))
	h := sqsd.NewStatHandler(tr)

	if m := h.BuildServeMux(); m == nil {
		t.Error("ServeMux not returned.")
	}
}
