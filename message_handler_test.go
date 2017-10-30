package sqsd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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
		h.OnJobAddedFunc(h, nil, ctx, wg)
		h.OnJobAddedFunc(h, job, ctx, wg)
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
		h.OnJobAddedFunc(h, job, parent, wg)
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
		h.OnJobAddedFunc(h, job, ctx, wg)
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
	c := &Conf{}
	r := NewResource(NewMockClient(), "http://example.com/foo/bar/queue")
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	ctx := context.Background()
	wg := &sync.WaitGroup{}

	h.HandleMessages(ctx, msgs, wg)

	if len(tr.CurrentWorkings) != tr.MaxProcessCount {
		t.Errorf("requests is wrong: %d", len(tr.CurrentWorkings))
	}

	if len(tr.Waitings) != 5 {
		t.Errorf("waiting count is wrong: %d", len(tr.Waitings))
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

func TestRunMainLoop(t *testing.T) {
	c := &Conf{}
	mc := NewMockClient()
	r := NewResource(mc, "http://example.com/foo/bar/queue")
	r.ReceiveParams.WaitTimeSeconds = aws.Int64(1)
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go h.RunMainLoop(ctx, wg)

	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestRunTrackerEventListener(t *testing.T) {
	c := &Conf{}
	mc := NewMockClient()
	r := NewResource(mc, "http://example.com/foo/bar/queue")
	r.ReceiveParams.WaitTimeSeconds = aws.Int64(1)
	tr := NewJobTracker(1)
	h := NewMessageHandler(r, tr, c)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	//origOnJobAddedFunc := h.OnJobAddedFunc
	origOnJobDeletedFunc := h.OnJobDeletedFunc

	mu := &sync.Mutex{}

	onjobAddedCount := 0
	onjobDeletedCount := 0
	h.OnJobAddedFunc = func(h *MessageHandler, job *Job, ctx context.Context, wg *sync.WaitGroup) {
		mu.Lock()
		onjobAddedCount++
		mu.Unlock()
	}
	h.OnJobDeletedFunc = func(h *MessageHandler, ctx context.Context, wg *sync.WaitGroup) {
		mu.Lock()
		onjobDeletedCount++
		mu.Unlock()
	}

	wg.Add(1)
	go h.RunTrackerEventListener(ctx, wg)
	time.Sleep(10 * time.Millisecond)

	job1 := NewJob(
		&sqs.Message{
			MessageId:     aws.String("id:1"),
			Body:          aws.String("foobarbaz"),
			ReceiptHandle: aws.String("hoge"),
		},
		h.Conf,
	)
	job2 := NewJob(
		&sqs.Message{
			MessageId:     aws.String("id:2"),
			Body:          aws.String("foobarbaz"),
			ReceiptHandle: aws.String("fuga"),
		},
		h.Conf,
	)

	added := tr.Add(job1)
	if !added {
		t.Error("job handling fails")
	}
	time.Sleep(5 * time.Millisecond)
	mu.Lock()
	if onjobAddedCount != 1 {
		t.Errorf("onjobaddedcount is wrong: %d\n", onjobAddedCount)
	}
	if onjobDeletedCount != 0 {
		t.Errorf("onjobdeletedcount is wrong: %d\n", onjobDeletedCount)
	}
	mu.Unlock()

	added = tr.Add(job2)
	if added {
		t.Error("job handling fails")
	}
	time.Sleep(5 * time.Millisecond)
	mu.Lock()
	if onjobAddedCount != 2 {
		t.Errorf("onjobaddedcount is wrong: %d\n", onjobAddedCount)
	}
	if onjobDeletedCount != 0 {
		t.Errorf("onjobdeletedcount is wrong: %d\n", onjobDeletedCount)
	}
	mu.Unlock()

	tr.Delete(job1)
	time.Sleep(5 * time.Millisecond)

	origOnJobDeletedFunc(h, ctx, wg)
	time.Sleep(5 * time.Millisecond)

	mu.Lock()
	if onjobAddedCount != 3 {
		t.Errorf("onjobaddedcount is wrong: %d\n", onjobAddedCount)
	}
	if onjobDeletedCount != 1 {
		t.Errorf("onjobdeletedcount is wrong: %d\n", onjobDeletedCount)
	}
	mu.Unlock()

	cancel()
	wg.Wait()
}

func TestMessageHandlerRun(t *testing.T) {
	h := NewMessageHandler(
		NewResource(NewMockClient(), "http://example.com/foo/bar/queue"),
		NewJobTracker(1),
		&Conf{},
	)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go h.Run(ctx, wg)
	time.Sleep(5 * time.Millisecond)

	cancel()
	wg.Done()
}