package sqsd

import (
	"errors"
	"fmt"
	"net/url"
	"os"
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

// WorkerConf is configuration parameters for request to worker endpoint.
type WorkerConf struct {
	Type            string          `toml:"type"`
	URL             string          `toml:"url"`
	MaxProcessCount uint            `toml:"max_process_count"`
	Healthcheck     HealthcheckConf `toml:"healthcheck"`
}

// HealthcheckConf is configuration parameters for healthcheck to worker before request to SQS.
type HealthcheckConf struct {
	URL           string `toml:"url"`
	MaxElapsedSec int64  `toml:"max_elapsed_sec"`
	MaxRequestMS  int64  `toml:"max_request_ms"`
}

// SQSConf provides parameters for request to sqs endpoint.
type SQSConf struct {
	AccountID   string `toml:"account_id"`
	QueueName   string `toml:"queue_name"`
	Region      string `toml:"region"`
	URL         string `toml:"url"`
	Concurrency uint   `toml:"concurrency"`
	WaitTimeSec int64  `toml:"wait_time_sec"`
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
	if c.MaxProcessCount == 0 {
		return errors.New("worker.max_process_count is required")
	}
	if c.Type != "http" {
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

func workerType(typ string) WorkerConfOption {
	return func(c *WorkerConf) {
		if c.Type == "" {
			c.Type = typ
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
	for _, o := range []WorkerConfOption{maxProcessCount(1), workerType("http")} {
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
func NewConf(filepath string) (*Conf, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	sqsdConf := &Conf{}
	if err := toml.NewDecoder(f).Decode(sqsdConf); err != nil {
		return nil, err
	}
	sqsdConf.Init()

	if err := sqsdConf.Validate(); err != nil {
		return nil, err
	}

	return sqsdConf, nil
}
