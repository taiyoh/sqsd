package sqsd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
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
	w := NewWorker(r, tr, c)
	if w == nil {
		t.Error("worker not loaded")
	}
}

func TestSetupJob(t *testing.T) {
	c := &Conf{}
	r := &Resource{}
	tr := NewJobTracker(5)
	w := NewWorker(r, tr, c)

	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	job := w.SetupJob(sqsMsg)
	if job == nil {
		t.Error("job not created")
	}
}

func TestHandleMessage(t *testing.T) {
	c := &Conf{}
	r := &Resource{Client: &MockClient{}}
	tr := NewJobTracker(5)
	w := NewWorker(r, tr, c)

	ctx := context.Background()

	ts := MockJobServer()
	defer ts.Close()

	wg := &sync.WaitGroup{}

	t.Run("job failed", func(t *testing.T) {
		job := w.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/error"

		wg.Add(1)
		go w.HandleMessage(ctx, job, wg)
		wg.Wait()
		if _, exists := w.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		job := w.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/long"
		parent, cancel := context.WithCancel(ctx)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go w.HandleMessage(parent, job, wg)
		cancel()
		wg.Wait()
		if _, exists := w.Tracker.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("success", func(t *testing.T) {
		job := w.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/ok"
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go w.HandleMessage(ctx, job, wg)
		wg.Wait()
		if _, exists := w.Tracker.CurrentWorkings[job.ID()]; exists {
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
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			p := DecodePayload(r.Body)
			caughtIds[p.ID] = true
			w.Header().Set("content-Type", "text")
			fmt.Fprintf(w, "goood")
			return
		},
	))
	defer ts.Close()

	c := &Conf{HTTPWorker: HttpWorkerConf{URL: ts.URL}}
	r := &Resource{Client: &MockClient{}}
	tr := NewJobTracker(5)
	w := NewWorker(r, tr, c)

	ctx := context.Background()

	wg := &sync.WaitGroup{}
	w.HandleMessages(ctx, msgs, wg)
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
	c := &Conf{SleepSeconds: 1}
	mc := NewMockClient()
	r := &Resource{Client: mc}
	tr := NewJobTracker(5)
	w := NewWorker(r, tr, c)

	funcEnds := make(chan bool)
	wg := &sync.WaitGroup{}
	run := func(ctx context.Context) {
		w.Run(ctx, wg)
		funcEnds <- true
	}

	t.Run("tracker is not working", func(t *testing.T) {
		tr.Pause()
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-funcEnds

		if mc.RecvRequestCount > 0 {
			t.Errorf("receive request count exists: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}

	})

	t.Run("received but empty messages -> context cancel", func(t *testing.T) {
		tr.Resume()
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-funcEnds

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.RecvRequestCount = 0
	mc.Err = errors.New("fugafuga")
	tr.JobWorking = true

	t.Run("error received", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)
		time.Sleep(50 * time.Millisecond)
		cancel()
		<-funcEnds

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.RecvRequestCount = 0
	mc.Err = nil
	for i := 1; i <= 3; i++ {
		idxStr := strconv.Itoa(i)
		p := &JobPayloadForTest{ID: "msgid:" + strconv.Itoa(i)}
		mc.Resp.Messages = append(mc.Resp.Messages, &sqs.Message{
			MessageId:     aws.String(p.ID),
			Body:          aws.String(p.String()),
			ReceiptHandle: aws.String("receithandle-" + idxStr),
		})
	}
	t.Run("request success", func(t *testing.T) {
		caughtIds := map[string]int{}
		ts := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				p := DecodePayload(r.Body)
				if _, exists := caughtIds[p.ID]; exists {
					caughtIds[p.ID]++
				} else {
					caughtIds[p.ID] = 1
				}
				w.Header().Set("content-Type", "text")
				fmt.Fprintf(w, "goood")
				return
			},
		))
		defer ts.Close()

		c.HTTPWorker.URL = ts.URL

		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go run(ctx)
		time.Sleep(100 * time.Millisecond)
		cancel()
		<-funcEnds

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
		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount != 3 {
			t.Errorf("delete request count wrong: %d", mc.RecvRequestCount)
		}
	})

	wg.Wait()
}
