package sqsd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

func TestNewWorker(t *testing.T) {
	c := &Conf{}
	r := &Resource{}
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)
	if h == nil {
		t.Error("worker not loaded")
	}
}

func TestSetupJob(t *testing.T) {
	c := &Conf{}
	r := &Resource{}
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	job := h.SetupJob(sqsMsg)
	if job == nil {
		t.Error("job not created")
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
		job := h.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/error"

		wg.Add(1)
		go h.HandleMessage(ctx, job, wg)
		wg.Wait()
		if _, exists := h.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		job := h.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/long"
		parent, cancel := context.WithCancel(ctx)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go h.HandleMessage(parent, job, wg)
		cancel()
		wg.Wait()
		if _, exists := h.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("success", func(t *testing.T) {
		job := h.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/ok"
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go h.HandleMessage(ctx, job, wg)
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

func TestWorkerRun(t *testing.T) {
	c := &Conf{}
	mc := NewMockClient()
	r := NewResource(mc, "http://example.com/foo/bar/queue")
	r.ReceiveParams.WaitTimeSeconds = aws.Int64(1)
	tr := NewJobTracker(5)
	h := NewMessageHandler(r, tr, c)

	wg := &sync.WaitGroup{}
	run := func(ctx context.Context) {
		h.Run(ctx, wg)
	}

	t.Run("tracker is not working", func(t *testing.T) {
		tr.Pause()
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)
		cancel()
		wg.Wait()

		if mc.RecvRequestCount != 0 {
			t.Errorf("receive request count exists: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.Err = nil
	mc.RecvRequestCount = 0
	tr.Resume()

	t.Run("received but empty messages -> context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)
		cancel()
		wg.Wait()

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.RecvRequestCount = 0
	mc.Err = errors.New("fugafuga")

	t.Run("error received", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)
		cancel()
		wg.Wait()

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	for i := 1; i <= 3; i++ {
		idxStr := strconv.Itoa(i)
		p := &JobPayloadForTest{ID: "msgid:" + strconv.Itoa(i)}
		mc.Resp.Messages = append(mc.Resp.Messages, &sqs.Message{
			MessageId:     aws.String(p.ID),
			Body:          aws.String(p.String()),
			ReceiptHandle: aws.String("receithandle-" + idxStr),
		})
	}
	mc.Err = nil
	mc.RecvFunc = func(param *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		mc.mu.Lock()
		mc.RecvRequestCount++
		mc.mu.Unlock()
		if mc.RecvRequestCount > 1 {
			mc.Resp.Messages = []*sqs.Message{}
			param.WaitTimeSeconds = aws.Int64(10)
		}
		if len(mc.Resp.Messages) == 0 && *param.WaitTimeSeconds > 0 {
			dur := time.Duration(*param.WaitTimeSeconds)
			time.Sleep(dur * time.Second)
		}
		return mc.Resp, mc.Err
	}

	r.ReceiveParams.WaitTimeSeconds = aws.Int64(2)
	t.Run("request success", func(t *testing.T) {
		caughtIds := map[string]int{}
		l := &sync.Mutex{}
		ts := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				p := DecodePayload(r.Body)
				log.Printf("task work: %s\n", p.ID)
				l.Lock()
				if _, exists := caughtIds[p.ID]; exists {
					caughtIds[p.ID]++
				} else {
					caughtIds[p.ID] = 1
				}
				l.Unlock()
				w.Header().Set("content-Type", "text")
				fmt.Fprintf(w, "goood")
				return
			},
		))
		defer ts.Close()

		h.Conf.JobURL = ts.URL

		ctx, _ := context.WithTimeout(context.Background(), 1)

		wg.Add(1)
		go run(ctx)
		//cancel()
		wg.Wait()

		if len(caughtIds) != 3 {
			t.Error("other request comes...")
		}
		for i := 1; i <= 3; i++ {
			id := "msgid:" + strconv.Itoa(i)
			v, exists := caughtIds[id]
			if !exists {
				t.Errorf("id: %s not requested", id)
			}
			if v != 1 {
				t.Errorf("id: %s over request: %d", id, v)
			}
		}
		if mc.RecvRequestCount != 2 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount != 3 {
			t.Errorf("delete request count wrong: %d", mc.RecvRequestCount)
		}
	})
}
