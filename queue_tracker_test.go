package sqsd

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestQueueTracker(t *testing.T) {
	tracker := NewQueueTracker(1, NewLogger("DEBUG"))
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	mu := new(sync.Mutex)

	now := time.Now()

	q1 := Queue{
		ID:         "id:1",
		Payload:    "hoge",
		Receipt:    "foo",
		ReceivedAt: now.Add(1),
	}
	q2 := Queue{
		ID:         "id:2",
		Payload:    "fuga",
		Receipt:    "bar",
		ReceivedAt: now,
	}

	allJobRegistered := false
	go func() {
		tracker.Register(q1)
		tracker.Register(q2)
		allJobRegistered = true
	}()

	time.Sleep(1 * time.Second)

	if allJobRegistered {
		t.Error("2 jobs inserted")
	}

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

	if !allJobRegistered {
		t.Error("second job not registered")
	}

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
	tr := NewQueueTracker(5, NewLogger("DEBUG"))

	if !tr.JobWorking {
		t.Error("JobWorking false")
	}

	tr.Pause()
	if tr.JobWorking {
		t.Error("JobWorking not changed to true")
	}

	tr.Resume()
	if !tr.JobWorking {
		t.Error("JobWorking not changed to false")
	}

	now := time.Now()

	q1 := Queue{
		ID:         "id:1",
		Payload:    "hoge",
		Receipt:    "foo",
		ReceivedAt: now,
	}
	q1Duplicates := Queue{
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
	tr := NewQueueTracker(5, NewLogger("DEBUG"))
	now := time.Now()
	for i := 1; i <= 2; i++ {
		iStr := strconv.Itoa(i)
		msg := &sqs.Message{
			MessageId:     aws.String("foo" + iStr),
			Body:          aws.String("bar" + iStr),
			ReceiptHandle: aws.String("baz" + iStr),
		}
		q := NewQueue(msg)
		q.ReceivedAt = now.Add(time.Duration(i))
		tr.Register(q)
	}

	summaries := tr.CurrentSummaries()
	for _, summary := range summaries {
		data, exists := tr.CurrentWorkings.Load(summary.ID)
		if !exists {
			t.Errorf("job not found: %s", summary.ID)
		}
		if summary.Payload != data.(Queue).Payload {
			t.Errorf("job payload is wrong: %s", summary.Payload)
		}
	}
}

func TestHealthCheck(t *testing.T) {
	tr := NewQueueTracker(5, NewLogger("DEBUG"))
	hc := HealthcheckConf{}

	if !tr.HealthCheck(hc) {
		t.Error("healthcheck should not support.")
	}

	ts := MockServer()
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
