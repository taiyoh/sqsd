package sqsd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
)

type QueueTracker struct {
	CurrentWorkings *sync.Map
	JobWorking      bool
	Logger          Logger
	queueChan       chan *Queue
	queueStack      chan struct{}
}

func NewQueueTracker(maxProcCount uint, logger Logger) *QueueTracker {
	procCount := int(maxProcCount)
	return &QueueTracker{
		CurrentWorkings: new(sync.Map),
		JobWorking:      true,
		Logger:          logger,
		queueChan:       make(chan *Queue, procCount),
		queueStack:      make(chan struct{}, procCount),
	}
}

func (t *QueueTracker) Register(q *Queue) {
	t.queueStack <- struct{}{} // blocking
	t.CurrentWorkings.Store(q.ID(), q)
	t.queueChan <- q
}

func (t *QueueTracker) Complete(q *Queue) {
	t.CurrentWorkings.Delete(q.ID())
	<-t.queueStack // unblock
}

func (t *QueueTracker) CurrentSummaries() []*QueueSummary {
	currentList := []*QueueSummary{}
	t.CurrentWorkings.Range(func(key, val interface{}) bool {
		currentList = append(currentList, (val.(*Queue)).Summary())
		return true
	})
	sort.Slice(currentList, func(i, j int) bool {
		return currentList[i].ReceivedAt < currentList[j].ReceivedAt
	})
	return currentList
}

func (t *QueueTracker) NextQueue() <-chan *Queue {
	return t.queueChan
}

func (t *QueueTracker) Pause() {
	t.JobWorking = false
}

func (t *QueueTracker) Resume() {
	t.JobWorking = true
}

func (t *QueueTracker) IsWorking() bool {
	return t.JobWorking
}

func (t *QueueTracker) HealthCheck(c HealthCheckConf) bool {
	if !c.ShouldSupport() {
		return true
	}

	ebo := backoff.NewExponentialBackOff()
	ebo.MaxElapsedTime = time.Duration(c.MaxElapsedSec) * time.Second
	client := &http.Client{}

	err := backoff.Retry(func() error {
		req, _ := http.NewRequest("GET", c.URL, bytes.NewBuffer([]byte("")))
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.MaxRequestMS)*time.Millisecond)
		defer cancel()
		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			t.Logger.Warn(fmt.Sprintf("healthcheck request failed. %s", err))
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logger.Warn(fmt.Sprintf("healthcheck response code != 200: %s", resp.Status))
			return errors.New("response status code != 200")
		}
		t.Logger.Info("healthcheck request success.")
		return nil
	}, ebo)
	if err != nil {
		return false
	}

	return true
}
