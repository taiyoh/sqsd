package sqsd

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/pelletier/go-toml"
)

// Conf aggregates configuration for sqsd.
type Conf struct {
	Main   MainConf   `toml:"main"`
	Worker WorkerConf `toml:"worker"`
	SQS    SQSConf    `toml:"sqs"`
}

// MainConf provides misc parameters for sqsd. not worker and sqs.
type MainConf struct {
	StatServerPort int    `toml:"stat_server_port"`
	LogLevel       string `toml:"log_level"`
}

// WorkerConf provides parameters for request to worker endpoint.
type WorkerConf struct {
	MaxProcessCount          uint   `toml:"max_process_count"`
	WorkerURL                string `toml:"worker_url"`
	HealthcheckURL           string `toml:"healthcheck_url"`
	HealthcheckMaxElapsedSec int64  `toml:"healthcheck_max_elapsed_sec"`
	HealthcheckMaxRequestMS  int64  `toml:"healthcheck_max_request_ms"`
}

// SQSConf provides parameters for request to sqs endpoint.
type SQSConf struct {
	AccountID   string `toml:"account_id"`
	QueueName   string `toml:"queue_name"`
	Region      string `toml:"region"`
	URL         string `toml:"url"`
	Concurrency uint   `toml:"concurrency"`
	WaitTimeSec uint   `toml:"wait_time_sec"`
}

// ConfSection is interface for Conf.Init and Conf.Validate
type ConfSection interface {
	Init(a ...interface{})
	Validate() error
}

// MainConfOption injects default value for MainConf
type MainConfOption func(c *MainConf)

// WorkerConfOption injects default value for WorkerConf
type WorkerConfOption func(c *WorkerConf)

// SQSConfOption injects default value for SQSConf
type SQSConfOption func(c *SQSConf)

// QueueURL builds sqs endpoint from sqs configuration
func (c SQSConf) QueueURL() string {
	var url string
	if c.URL != "" {
		url = c.URL
	} else {
		url = fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", c.Region, c.AccountID, c.QueueName)
	}
	return url
}

// ShouldHealthcheckSupport returns either healthcheck_url is registered or not.
func (c WorkerConf) ShouldHealthcheckSupport() bool {
	return c.HealthcheckURL != ""
}

// Init processes MainConfOption typed functions for injecting default value
func (c MainConf) Init(opts ...interface{}) {
	for _, o := range opts {
		o.(MainConfOption)(&c)
	}
}

// Init processes WorkerConfOption typed functions for injecting default value
func (c WorkerConf) Init(opts ...interface{}) {
	for _, o := range opts {
		o.(WorkerConfOption)(&c)
	}
}

// Init processes SQSConfOption typed functions for injecting default value
func (c SQSConf) Init(opts ...interface{}) {
	for _, o := range opts {
		o.(SQSConfOption)(&c)
	}
}

// <!-- validation section start

func isURL(urlStr string) bool {
	uri, err := url.ParseRequestURI(urlStr)
	return err == nil && strings.HasPrefix(uri.Scheme, "http")
}

// Validate returns error if worker configuration is invalid.
func (c WorkerConf) Validate() error {
	if !isURL(c.WorkerURL) {
		return errors.New("worker.worker_url is not HTTP URL: " + c.WorkerURL)
	}

	if c.HealthcheckURL != "" {
		if !isURL(c.HealthcheckURL) {
			return errors.New("worker.healthcheck_url is not HTTP URL: " + c.HealthcheckURL)
		}
		if c.HealthcheckMaxElapsedSec == 0 {
			return errors.New("worker.healthcheck_max_elapsed_sec is required")
		}
	}

	return nil
}

// Validate returns error if sqs configuration is invalid.
func (c SQSConf) Validate() error {
	if c.URL == "" {
		if c.AccountID == "" {
			return errors.New("sqs.account_id is required")
		}
		if c.QueueName == "" {
			return errors.New("sqs.queue_name is required")
		}
	} else {
		if !isURL(c.URL) {
			return errors.New("sqs.url is not HTTP URL: " + c.URL)
		}
	}
	if c.Region == "" {
		return errors.New("sqs.region is required")
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

func waitTimeSec(s uint) SQSConfOption {
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

func logLevel(l string) MainConfOption {
	return func(c *MainConf) {
		if c.LogLevel == "" {
			c.LogLevel = l
		}
	}
}

func healthcheckMaxRequestMillisec(ms int64) WorkerConfOption {
	return func(c *WorkerConf) {
		if c.HealthcheckURL != "" && c.HealthcheckMaxRequestMS == 0 {
			c.HealthcheckMaxRequestMS = ms
		}
	}
}

func concurrency(i uint) SQSConfOption {
	return func(c *SQSConf) {
		if c.Concurrency == 0 {
			c.Concurrency = 1
		}
	}
}

// default value section ends -->

// Init fills default values for each sections.
func (c *Conf) Init() {
	c.Main.Init(logLevel("INFO"))
	c.Worker.Init(maxProcessCount(1), healthcheckMaxRequestMillisec(1000))
	c.SQS.Init(waitTimeSec(20), concurrency(1))
}

// Validate processes Validate method for each sections. return error if exists.
func (c *Conf) Validate() error {
	confs := []ConfSection{c.Main, c.Worker, c.SQS}
	for _, c := range confs {
		if err := c.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// NewConf returns aggregated sqsd configuration object.
func NewConf(filepath string) (*Conf, error) {
	config, err := toml.LoadFile(filepath)
	if err != nil {
		return nil, err
	}

	sqsdConf := &Conf{}
	config.Unmarshal(sqsdConf)
	sqsdConf.Init()

	if err := sqsdConf.Validate(); err != nil {
		return nil, err
	}

	return sqsdConf, nil
}
