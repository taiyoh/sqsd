package sqsd

import (
	"bytes"
	"fmt"
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

type macopy struct{}

func (*macopy) Lock() {}

// ScoreBoard represents executed worker count manager.
type ScoreBoard struct {
	TotalSucceeded int64
	TotalFailed    int64
	MaxWorker      int
	noMacopy       macopy
}

// TotalHandled returns all success and fail counts.
func (s *ScoreBoard) TotalHandled() int64 {
	return s.TotalSucceeded + s.TotalFailed
}

// ReportSuccess provides increment success count.
func (s *ScoreBoard) ReportSuccess() {
	atomic.AddInt64(&s.TotalSucceeded, 1)
}

// ReportFail provides increment fail count.
func (s *ScoreBoard) ReportFail() {
	atomic.AddInt64(&s.TotalFailed, 1)
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
func (t *QueueTracker) Complete(q Queue) {
	t.CurrentWorkings.Delete(q.ID)
	if q.ResultStatus == RequestFail {
		t.ScoreBoard.ReportFail()
	} else if q.ResultStatus == RequestSuccess {
		t.ScoreBoard.ReportSuccess()
	}
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
	client := &http.Client{
		Timeout: time.Duration(c.MaxRequestMS) * time.Millisecond,
	}
	req, _ := http.NewRequest(http.MethodGet, c.URL, bytes.NewBuffer([]byte{}))

	for b.Continue() {
		if err := t.healthcheckRequest(client, req); err != nil {
			t.Logger.Warnf("healthcheck request failed: %v", err)
			continue
		}
		t.Logger.Info("healthcheck request success.")
		return true
	}
	return false
}

func (t *QueueTracker) healthcheckRequest(client *http.Client, req *http.Request) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("response code != 200: %s", resp.Status)
	}
	return nil
}
