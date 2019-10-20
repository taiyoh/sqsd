package sqsd_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
)

func TestQueueTracker(t *testing.T) {
	tracker := sqsd.NewQueueTracker(1, sqsd.NewLogger("DEBUG"))
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	var mu sync.Mutex

	now := time.Now()

	q1 := sqsd.Queue{
		ID:         "id:1",
		Payload:    "hoge",
		Receipt:    "foo",
		ReceivedAt: now.Add(1),
	}
	q2 := sqsd.Queue{
		ID:         "id:2",
		Payload:    "fuga",
		Receipt:    "bar",
		ReceivedAt: now,
	}

	allJobRegistered := false
	go func() {
		tracker.Register(q1)
		tracker.Register(q2)
		mu.Lock()
		allJobRegistered = true
		mu.Unlock()
	}()

	time.Sleep(1 * time.Second)

	mu.Lock()
	if allJobRegistered {
		t.Error("2 jobs inserted")
	}
	mu.Unlock()

	receivedQueue := <-tracker.NextQueue()
	if receivedQueue.ID != q1.ID {
		t.Error("wrong order")
	}

	summaries := tracker.CurrentSummaries()
	if summaries[0].ID != q1.ID {
		t.Error("wrong order")
	}

	tracker.Complete(receivedQueue)

	time.Sleep(1 * time.Second)

	mu.Lock()
	if !allJobRegistered {
		t.Error("second job not registered")
	}
	mu.Unlock()

	receivedQueue = <-tracker.NextQueue()
	if receivedQueue.ID != q2.ID {
		t.Error("wrong order")
	}

	summaries = tracker.CurrentSummaries()
	if summaries[0].ID != q2.ID {
		t.Error("wrong order")
	}
}

func TestJobWorking(t *testing.T) {
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))

	if !tr.IsWorking() {
		t.Error("JobWorking false")
	}

	tr.Pause()
	if tr.IsWorking() {
		t.Error("JobWorking not changed to true")
	}

	tr.Resume()
	if !tr.IsWorking() {
		t.Error("JobWorking not changed to false")
	}

	now := time.Now()

	q1 := sqsd.Queue{
		ID:         "id:1",
		Payload:    "hoge",
		Receipt:    "foo",
		ReceivedAt: now,
	}
	q1Duplicates := sqsd.Queue{
		ID:         "id:1",
		Payload:    "fuga",
		Receipt:    "bar",
		ReceivedAt: now.Add(3),
	}
	tr.Register(q1)
	tr.Register(q1Duplicates)

	summaries := tr.CurrentSummaries()
	if len(summaries) != 1 {
		t.Error("both queue are registered!")
	}
}

func TestCurrentSummaries(t *testing.T) {
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))
	now := time.Now()
	for i := 1; i <= 2; i++ {
		iStr := strconv.Itoa(i)
		msg := &sqs.Message{
			MessageId:     aws.String("foo" + iStr),
			Body:          aws.String("bar" + iStr),
			ReceiptHandle: aws.String("baz" + iStr),
		}
		q := sqsd.NewQueue(msg)
		q.ReceivedAt = now.Add(time.Duration(i))
		tr.Register(q)
	}

	summaries := tr.CurrentSummaries()
	for _, summary := range summaries {
		data, exists := tr.Find(summary.ID)
		if !exists {
			t.Errorf("job not found: %s", summary.ID)
		}
		if summary.Payload != data.Payload {
			t.Errorf("job payload is wrong: %s", summary.Payload)
		}
	}
}

func TestHealthCheck(t *testing.T) {
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))
	hc := sqsd.HealthcheckConf{}

	if !tr.HealthCheck(hc) {
		t.Error("healthcheck should not support.")
	}

	ts := sqsd.MockServer()
	defer ts.Close()

	t.Run("error returns", func(t *testing.T) {
		hc.URL = ts.URL + "/error"
		hc.MaxElapsedSec = 1
		hc.MaxRequestMS = 1000
		if tr.HealthCheck(hc) {
			t.Error("healthcheck is success. but expected failure.")
		}
	})

	t.Run("request timeout", func(t *testing.T) {
		hc.URL = ts.URL + "/long"
		hc.MaxElapsedSec = 2
		hc.MaxRequestMS = 300
		if tr.HealthCheck(hc) {
			t.Error("healthcheck is success. but expected failure.")
		}
	})

	t.Run("response ok", func(t *testing.T) {
		hc.URL = ts.URL + "/ok"
		hc.MaxElapsedSec = 3
		hc.MaxRequestMS = 1000
		if !tr.HealthCheck(hc) {
			t.Error("healthcheck is failure. but expected success.")
		}
	})

}
