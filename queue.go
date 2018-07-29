package sqsd

import (
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type QueueResultStatus int

const (
	NotRequested   = iota // default
	RequestSuccess = iota
	RequestFail    = iota
)

// Queue provides transition from sqs.Message
type Queue struct {
	ID           string
	Payload      string
	Receipt      string
	ReceivedAt   time.Time
	ResultStatus QueueResultStatus
}

func (q Queue) ResultSucceeded() Queue {
	return Queue{
		ID:           q.ID,
		Payload:      q.Payload,
		Receipt:      q.Receipt,
		ReceivedAt:   q.ReceivedAt,
		ResultStatus: RequestSuccess,
	}
}

func (q Queue) ResultFailed() Queue {
	return Queue{
		ID:           q.ID,
		Payload:      q.Payload,
		Receipt:      q.Receipt,
		ReceivedAt:   q.ReceivedAt,
		ResultStatus: RequestFail,
	}
}

// QueueSummary provides transition from Queue for stat
type QueueSummary struct {
	ID         string `json:"id"`
	ReceivedAt int64  `json:"received_at"`
	Payload    string `json:"payload"`
}

// NewQueue returns Queue object from sqs.Message
func NewQueue(msg *sqs.Message) Queue {
	return Queue{
		ID:         *msg.MessageId,
		Payload:    *msg.Body,
		Receipt:    *msg.ReceiptHandle,
		ReceivedAt: time.Now(),
	}
}

// Summary returns QueueSummary object from Queue
func (q Queue) Summary() QueueSummary {
	return QueueSummary{
		ID:         q.ID,
		ReceivedAt: q.ReceivedAt.Unix(),
		Payload:    q.Payload,
	}
}
