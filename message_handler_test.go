package sqsd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type JobPayloadForTest struct {
	ID string
}

func (p *JobPayloadForTest) String() string {
	buf, _ := json.Marshal(p)
	return bytes.NewBuffer(buf).String()
}
func DecodePayload(body io.ReadCloser) *JobPayloadForTest {
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	var p JobPayloadForTest
	json.Unmarshal(buf.Bytes(), &p)
	return &p
}

func TestNewMessageHandler(t *testing.T) {
	c := &Conf{}
	r := &Resource{}
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)
	if h == nil {
		t.Error("worker not loaded")
	}
}

func TestHandleMessage(t *testing.T) {
	c := &Conf{}
	r := NewResource(NewMockClient(), "http://example.com/foo/bar/queue")
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	ctx := context.Background()

	ts := MockJobServer()
	defer ts.Close()

	wg := &sync.WaitGroup{}

	t.Run("job failed", func(t *testing.T) {
		job := NewJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		}, h.Conf)
		job.URL = ts.URL + "/error"

		wg.Add(1)
		go h.HandleJob(ctx, job, wg)
		wg.Wait()
		if _, exists := h.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		job := NewJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		}, h.Conf)
		job.URL = ts.URL + "/long"
		parent, cancel := context.WithCancel(ctx)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go h.HandleJob(parent, job, wg)
		cancel()
		wg.Wait()
		if _, exists := h.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("success", func(t *testing.T) {
		job := NewJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		}, h.Conf)
		job.URL = ts.URL + "/ok"
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go h.HandleJob(ctx, job, wg)
		wg.Wait()
		if _, exists := h.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	wg.Wait()
}

func TestHandleMessages(t *testing.T) {
	msgs := []*sqs.Message{}
	for i := 1; i <= 10; i++ {
		idxStr := strconv.Itoa(i)
		p := &JobPayloadForTest{ID: "msgid:" + strconv.Itoa(i)}
		msgs = append(msgs, &sqs.Message{
			MessageId:     aws.String(p.ID),
			Body:          aws.String(p.String()),
			ReceiptHandle: aws.String("receithandle-" + idxStr),
		})
	}
	caughtIds := map[string]bool{}
	l := &sync.Mutex{}
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			p := DecodePayload(r.Body)
			l.Lock()
			caughtIds[p.ID] = true
			l.Unlock()
			w.Header().Set("content-Type", "text")
			fmt.Fprintf(w, "goood")
			return
		},
	))
	defer ts.Close()

	c := &Conf{Worker: WorkerConf{JobURL: ts.URL}}
	r := NewResource(NewMockClient(), "http://example.com/foo/bar/queue")
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	ctx := context.Background()

	wg := &sync.WaitGroup{}
	h.HandleMessages(ctx, msgs, wg)
	wg.Wait()

	if len(caughtIds) != tr.MaxProcessCount {
		t.Errorf("requests is wrong: %d", len(caughtIds))
	}

	for i := 6; i <= 10; i++ {
		id := "msgid:" + strconv.Itoa(i)
		if _, exists := caughtIds[id]; exists {
			t.Errorf("id: %s exists", id)
		}
	}
}

func TestDoHandle(t *testing.T) {
	c := &Conf{}
	mc := NewMockClient()
	r := NewResource(mc, "http://example.com/foo/bar/queue")
	r.ReceiveParams.WaitTimeSeconds = aws.Int64(1)
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	wg := &sync.WaitGroup{}

	handleEmptyCalled := false

	h.HandleEmptyFunc = func() {
		handleEmptyCalled = true
	}
	mc.ErrRequestCount = 0
	tr.Pause()
	if tr.JobWorking {
		t.Error("jobworking not changed")
	}
	t.Run("tracker is not working", func(t *testing.T) {
		h.DoHandle(context.Background(), wg)

		if !handleEmptyCalled {
			t.Error("HandleEmpty not working")
		}
		if mc.ErrRequestCount != 0 {
			t.Error("error exists")
		}
		if mc.RecvRequestCount != 0 {
			t.Errorf("receive request count exists: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	tr.Resume()

	mc.Err = errors.New("hogehoge")
	mc.RecvRequestCount = 0
	mc.ErrRequestCount = 0
	h.ShouldStop = false
	handleEmptyCalled = false
	if !tr.JobWorking {
		t.Error("jobworking flag not changed")
	}
	t.Run("received but empty messages", func(t *testing.T) {
		h.DoHandle(context.Background(), wg)

		if !handleEmptyCalled {
			t.Error("HandleEmpty not working")
		}

		if mc.ErrRequestCount != 1 {
			t.Error("no error")
		}

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.Err = nil
	mc.RecvRequestCount = 0
	mc.ErrRequestCount = 0
	handleEmptyCalled = false
	t.Run("received but empty messages", func(t *testing.T) {
		h.DoHandle(context.Background(), wg)

		if !handleEmptyCalled {
			t.Error("HandleEmpty not working")
		}
		if mc.ErrRequestCount != 0 {
			t.Error("error exists")
		}

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.Resp.Messages = []*sqs.Message{
		&sqs.Message{
			MessageId: aws.String("id:1"),
			Body:      aws.String(`foobar`),
		},
	}
	mc.Err = nil
	mc.RecvRequestCount = 0
	handleEmptyCalled = false
	t.Run("received 1 message", func(t *testing.T) {
		var receivedjob *Job
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case receivedjob = <-tr.JobAdded():
					return
				}
			}
		}()
		time.Sleep(5 * time.Millisecond)

		h.DoHandle(context.Background(), wg)
		wg.Wait()

		if handleEmptyCalled {
			t.Error("HandleEmpty worked")
		}
		if mc.ErrRequestCount != 0 {
			t.Error("error exists")
		}
		if receivedjob == nil {
			t.Error("job not received")
		}
		if receivedjob.ID() != *mc.Resp.Messages[0].MessageId {
			t.Error("wrong job received")
		}

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})
}
