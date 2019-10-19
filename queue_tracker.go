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
	currentWorkings *sync.Map
	jobWorking      bool
	logger          Logger
	queueChan       chan Queue
	queueStack      chan struct{}
	scoreBoard      scoreBoard
}

type macopy struct{}

func (*macopy) Lock() {}

// ScoreBoard represents executed worker count manager.
type scoreBoard struct {
	TotalSucceeded int64
	TotalFailed    int64
	MaxWorker      int
	noMacopy       macopy
}

// TotalHandled returns all success and fail counts.
func (s *scoreBoard) TotalHandled() int64 {
	return s.TotalSucceeded + s.TotalFailed
}

// ReportSuccess provides increment success count.
func (s *scoreBoard) ReportSuccess() {
	atomic.AddInt64(&s.TotalSucceeded, 1)
}

// ReportFail provides increment fail count.
func (s *scoreBoard) ReportFail() {
	atomic.AddInt64(&s.TotalFailed, 1)
}

// NewQueueTracker returns QueueTracker object
func NewQueueTracker(maxProcCount uint, logger Logger) *QueueTracker {
	procCount := int(maxProcCount)
	return &QueueTracker{
		currentWorkings: &sync.Map{},
		jobWorking:      true,
		logger:          logger,
		queueChan:       make(chan Queue, procCount),
		queueStack:      make(chan struct{}, procCount),
		scoreBoard: scoreBoard{
			MaxWorker: procCount,
		},
	}
}

// Register provides registering queues to tracker. But existing queue is ignored. And when queue stack is filled, wait until a slot opens up
func (t *QueueTracker) Register(q Queue) {
	if _, loaded := t.currentWorkings.LoadOrStore(q.ID, q); !loaded {
		t.queueStack <- struct{}{} // for blocking
		t.queueChan <- q
	}
}

// Complete provides finalizing queue tracking. Deleting queue from itself and opening up one queue stack
func (t *QueueTracker) Complete(q Queue) {
	t.currentWorkings.Delete(q.ID)
	if q.ResultStatus == RequestFail {
		t.scoreBoard.ReportFail()
	} else if q.ResultStatus == RequestSuccess {
		t.scoreBoard.ReportSuccess()
	}
	<-t.queueStack // unblock
}

// CurrentSummaries returns QueueSummary list from its owned
func (t *QueueTracker) CurrentSummaries() []QueueSummary {
	currentList := []QueueSummary{}
	t.currentWorkings.Range(func(key, val interface{}) bool {
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
	t.jobWorking = false
}

// Resume provides starting recieving queues
func (t *QueueTracker) Resume() {
	t.jobWorking = true
}

// IsWorking returns whether recieving queue is alive or not
func (t *QueueTracker) IsWorking() bool {
	return t.jobWorking
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
			t.logger.Warnf("healthcheck request failed: %v", err)
			continue
		}
		t.logger.Info("healthcheck request success.")
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
