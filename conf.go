package sqsd

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/kelseyhightower/envconfig"
)

// Conf aggregates configuration for sqsd.
type Conf struct {
	Main   MainConf
	Worker WorkerConf
	SQS    SQSConf
}

// MainConf provides misc parameters for sqsd. not worker and sqs.
type MainConf struct {
	StatServerPort int    `envconfig:"STAT_SERVER_PORT" default:"5956"`
	LogLevel       string `envconfig:"LOG_LEVEL"        default:"INFO"`
}

// WorkerConf is configuration parameters for request to worker endpoint.
type WorkerConf struct {
	InvokeType      string `envconfig:"WORKER_INVOKE_TYPE"       default:"http"`
	URL             string `envconfig:"WORKER_DESTINATION_URL"   required:"true"`
	MaxProcessCount uint   `envconfig:"WORKER_MAX_PROCESS_COUNT" default:"1"`
	Healthcheck     HealthcheckConf
}

// HealthcheckConf is configuration parameters for healthcheck to worker before request to SQS.
type HealthcheckConf struct {
	URL           string `envconfig:"HEALTHCHECK_URL"`
	MaxElapsedSec int64  `envconfig:"HEALTHCHECK_MAX_ELAPSED_SECONDS" default:"10"`
	MaxRequestMS  int64  `envconfig:"HEALTHCHECK_MAX_REQUEST_MS"      default:"1000"`
}

// SQSConf provides parameters for request to sqs endpoint.
type SQSConf struct {
	AccountID   string `envconfig:"AWS_ACCESS_KEY_ID"`
	Region      string `envconfig:"AWS_REGION"            default:"us-east-1"`
	QueueName   string `envconfig:"SQS_QUEUE_NAME"`
	URL         string `envconfig:"SQS_QUEUE_URL"`
	Concurrency uint   `envconfig:"SQS_CONCURRENCY"       default:"1"`
	WaitTimeSec int64  `envconfig:"SQS_WAIT_TIME_SECONDS" default:"20"`
}

type confSection interface {
	Validate() error
}

// MainConfOption injects default value for MainConf
type MainConfOption func(c *MainConf)

// WorkerConfOption injects default value for WorkerConf
type WorkerConfOption func(c *WorkerConf)

// HealthcheckConfOption injects default value for HealthcheckConf
type HealthcheckConfOption func(c *HealthcheckConf)

// SQSConfOption injects default value for SQSConf
type SQSConfOption func(c *SQSConf)

// QueueURL builds sqs endpoint from sqs configuration
func (c SQSConf) QueueURL() string {
	if c.URL != "" {
		return c.URL
	}
	return fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", c.Region, c.AccountID, c.QueueName)
}

// ShouldSupport returns either healthcheck_url is registered or not.
func (c HealthcheckConf) ShouldSupport() bool {
	return c.URL != ""
}

// <!-- validation section start

func isURL(urlStr string) bool {
	uri, err := url.ParseRequestURI(urlStr)
	return err == nil && strings.HasPrefix(uri.Scheme, "http")
}

// Validate returns error if worker configuration is invalid.
func (c WorkerConf) Validate() error {
	if c.InvokeType != "http" {
		return nil
	}
	if !isURL(c.URL) {
		return errors.New("worker.url is not HTTP URL: " + c.URL)
	}

	if c.Healthcheck.URL != "" {
		if !isURL(c.Healthcheck.URL) {
			return errors.New("worker.healthcheck.url is not HTTP URL: " + c.Healthcheck.URL)
		}
		if c.Healthcheck.MaxElapsedSec == 0 {
			return errors.New("worker.healthcheck.max_elapsed_sec is required")
		}
	}

	return nil
}

// Validate returns error if sqs configuration is invalid.
func (c SQSConf) Validate() error {
	if c.Region == "" {
		return errors.New("sqs.region is required")
	}
	if c.URL != "" {
		if !isURL(c.URL) {
			return errors.New("sqs.url is not HTTP URL: " + c.URL)
		}
		return nil
	}
	if c.AccountID == "" {
		return errors.New("sqs.account_id is required")
	}
	if c.QueueName == "" {
		return errors.New("sqs.queue_name is required")
	}
	return nil
}

// Validate returns error if main configuration is invalid.
func (c MainConf) Validate() error {
	levelMap := map[string]struct{}{
		"DEBUG": struct{}{},
		"INFO":  struct{}{},
		"WARN":  struct{}{},
		"ERROR": struct{}{},
	}
	if _, ok := levelMap[c.LogLevel]; !ok {
		return errors.New("main.log_level is invalid: " + c.LogLevel)
	}

	if c.StatServerPort == 0 {
		return errors.New("main.stat_server_port is required")
	}

	return nil
}

// validation section ends -->

// <!-- default value section start

func waitTimeSec(s int64) SQSConfOption {
	return func(c *SQSConf) {
		if c.WaitTimeSec == 0 {
			c.WaitTimeSec = s
		}
	}
}

func maxProcessCount(i uint) WorkerConfOption {
	return func(c *WorkerConf) {
		if c.MaxProcessCount == 0 {
			c.MaxProcessCount = i
		}
	}
}

func workerInvokeType(typ string) WorkerConfOption {
	return func(c *WorkerConf) {
		if c.InvokeType == "" {
			c.InvokeType = typ
		}
	}
}

func logLevel(l string) MainConfOption {
	return func(c *MainConf) {
		if c.LogLevel == "" {
			c.LogLevel = l
		}
	}
}

func maxRequestMillisec(ms int64) HealthcheckConfOption {
	return func(c *HealthcheckConf) {
		if c.URL != "" && c.MaxRequestMS == 0 {
			c.MaxRequestMS = ms
		}
	}
}

func concurrency(i uint) SQSConfOption {
	return func(c *SQSConf) {
		if c.Concurrency == 0 {
			c.Concurrency = i
		}
	}
}

// default value section ends -->

// Init fills default values for each sections.
func (c *Conf) Init() {
	for _, o := range []MainConfOption{logLevel("INFO")} {
		o(&c.Main)
	}
	for _, o := range []WorkerConfOption{maxProcessCount(1), workerInvokeType("http")} {
		o(&c.Worker)
	}
	for _, o := range []HealthcheckConfOption{maxRequestMillisec(1000)} {
		o(&c.Worker.Healthcheck)
	}
	for _, o := range []SQSConfOption{waitTimeSec(20), concurrency(1)} {
		o(&c.SQS)
	}
}

// Validate processes Validate method for each sections. return error if exists.
func (c *Conf) Validate() error {
	for _, c := range []confSection{c.Main, c.Worker, c.SQS} {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// NewConf returns aggregated sqsd configuration object.
func NewConf() (*Conf, error) {
	sqsdConf := &Conf{}
	if err := envconfig.Process("", sqsdConf); err != nil {
		return nil, err
	}
	sqsdConf.Init()

	if err := sqsdConf.Validate(); err != nil {
		return nil, err
	}

	return sqsdConf, nil
}
