package sqsd

import (
	"bytes"
	"context"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// QueueTracker provides recieving queues from MessageProvider, and sending queues to MessageConsumer
type QueueTracker struct {
	CurrentWorkings *sync.Map
	JobWorking      bool
	Logger          Logger
	queueChan       chan Queue
	queueStack      chan struct{}
	ScoreBoard      ScoreBoard
}

type ScoreBoard struct {
	TotalSucceeded int64
	TotalFailed    int64
	MaxWorker      int
}

func (s *ScoreBoard) ReportScore(success bool) {
	if success {
		atomic.AddInt64(&s.TotalSucceeded, 1)
	} else {
		atomic.AddInt64(&s.TotalFailed, 1)
	}
}

// NewQueueTracker returns QueueTracker object
func NewQueueTracker(maxProcCount uint, logger Logger) *QueueTracker {
	procCount := int(maxProcCount)
	return &QueueTracker{
		CurrentWorkings: &sync.Map{},
		JobWorking:      true,
		Logger:          logger,
		queueChan:       make(chan Queue, procCount),
		queueStack:      make(chan struct{}, procCount),
		ScoreBoard: ScoreBoard{
			MaxWorker: procCount,
		},
	}
}

// Register provides registering queues to tracker. But existing queue is ignored. And when queue stack is filled, wait until a slot opens up
func (t *QueueTracker) Register(q Queue) {
	if _, exists := t.CurrentWorkings.Load(q.ID); !exists {
		t.queueStack <- struct{}{} // blocking
		t.CurrentWorkings.Store(q.ID, q)
		t.queueChan <- q
	}
}

// Complete provides finalizing queue tracking. Deleting queue from itself and opening up one queue stack
func (t *QueueTracker) Complete(q Queue, ok bool) {
	t.CurrentWorkings.Delete(q.ID)
	t.ScoreBoard.ReportScore(ok)
	<-t.queueStack // unblock
}

// CurrentSummaries returns QueueSummary list from its owned
func (t *QueueTracker) CurrentSummaries() []QueueSummary {
	currentList := []QueueSummary{}
	t.CurrentWorkings.Range(func(key, val interface{}) bool {
		currentList = append(currentList, (val.(Queue)).Summary())
		return true
	})
	sort.Slice(currentList, func(i, j int) bool {
		return currentList[i].ReceivedAt < currentList[j].ReceivedAt
	})
	return currentList
}

// NextQueue returns channel for MessageConsumer
func (t *QueueTracker) NextQueue() <-chan Queue {
	return t.queueChan
}

// Pause provides stopping receiving queues
func (t *QueueTracker) Pause() {
	t.JobWorking = false
}

// Resume provides starting recieving queues
func (t *QueueTracker) Resume() {
	t.JobWorking = true
}

// IsWorking returns whether recieving queue is alive or not
func (t *QueueTracker) IsWorking() bool {
	return t.JobWorking
}

// HealthCheck provides checking worker status using specified endpoing.
func (t *QueueTracker) HealthCheck(c HealthcheckConf) bool {
	if !c.ShouldSupport() {
		return true
	}

	b := NewBackOff(c.MaxElapsedSec)
	client := &http.Client{}

	for b.Continue() {
		req, _ := http.NewRequest("GET", c.URL, bytes.NewBuffer([]byte("")))
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.MaxRequestMS)*time.Millisecond)
		defer cancel()
		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			t.Logger.Warnf("healthcheck request failed. %s", err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logger.Warnf("healthcheck response code != 200: %s", resp.Status)
			continue
		}
		t.Logger.Info("healthcheck request success.")
		return true
	}
	return false
}
