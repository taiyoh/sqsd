package sqsd

import (
	"errors"
	"net/url"
	"strings"

	"github.com/pelletier/go-toml"
)

type workerConfOption func(c *WorkerConf) error
type sqsConfOption func(c *SQSConf) error
type healthcheckConfOption func(c *HealthCheckConf) error

type Conf struct {
	Worker      WorkerConf      `toml:"worker"`
	HealthCheck HealthCheckConf `toml:"healthcheck"`
	Stat        StatConf        `toml:"stat"`
	SQS         SQSConf         `toml:"sqs"`
}

type WorkerConf struct {
	IntervalSeconds uint64 `toml:"interval_seconds"`
	MaxProcessCount uint   `toml:"max_process_count"`
	WorkerURL       string `toml:"worker_url"`
	LogLevel        string `toml:"log_level"`
}

type HealthCheckConf struct {
	URL           string `toml:"url"`
	MaxElapsedSec int64  `toml:"max_elapsed_sec"`
	MaxRequestMS  int64  `toml:"max_request_ms"`
}

func isURL(urlStr string) bool {
	uri, err := url.ParseRequestURI(urlStr)
	return err == nil && strings.HasPrefix(uri.Scheme, "http")
}

func (c WorkerConf) Validate() error {
	if !isURL(c.WorkerURL) {
		return errors.New("worker.worker_url is not HTTP URL: " + c.WorkerURL)
	}
	levelMap := map[string]struct{}{
		"DEBUG": struct{}{},
		"INFO":  struct{}{},
		"WARN":  struct{}{},
		"ERROR": struct{}{},
	}
	if _, ok := levelMap[c.LogLevel]; !ok {
		return errors.New("worker.log_level is invalid: " + c.LogLevel)
	}
	return nil
}

type StatConf struct {
	ServerPort int `toml:"server_port"`
}

// https://sqs.<region>.amazonaws.com/<account_id>/<queue_name>"
type SQSConf struct {
	AccountID   string `toml:"account_id"`
	QueueName   string `toml:"queue_name"`
	Region      string `toml:"region"`
	URL         string `toml:"url"`
	Concurrency uint   `toml:"concurrency"`
}

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

func (c SQSConf) QueueURL() string {
	var url string
	if c.URL != "" {
		url = c.URL
	} else {
		url = "https://sqs." + c.Region + ".amazonaws.com/" + c.AccountID + "/" + c.QueueName
	}
	return url
}

func (c HealthCheckConf) Validate() error {
	if c.URL != "" {
		if !isURL(c.URL) {
			return errors.New("healthcheck.url is not HTTP URL: " + c.URL)
		}
		if c.MaxElapsedSec == 0 {
			return errors.New("healthcheck.max_elapsed_sec is required")
		}
	}
	return nil
}

func (c HealthCheckConf) ShouldSupport() bool {
	return c.URL != ""
}

func intervalSec(s uint64) workerConfOption {
	return func(c *WorkerConf) error {
		if c.IntervalSeconds == 0 {
			c.IntervalSeconds = s
		}
		return nil
	}
}

func maxProcessCount(i uint) workerConfOption {
	return func(c *WorkerConf) error {
		if c.MaxProcessCount == 0 {
			c.MaxProcessCount = i
		}
		return nil
	}
}

func logLevel(l string) workerConfOption {
	return func(c *WorkerConf) error {
		if c.LogLevel == "" {
			c.LogLevel = l
		}
		return nil
	}
}

func maxRequestMillisec(ms int64) healthcheckConfOption {
	return func(c *HealthCheckConf) error {
		if c.URL != "" && c.MaxRequestMS == 0 {
			c.MaxRequestMS = ms
		}
		return nil
	}
}

func concurrency(i uint) sqsConfOption {
	return func(c *SQSConf) error {
		if c.Concurrency == 0 {
			c.Concurrency = 1
		}
		return nil
	}
}

func (c *Conf) workerInit(opts ...workerConfOption) {
	for _, o := range opts {
		o(&c.Worker)
	}
}

func (c *Conf) healthcheckInit(opts ...healthcheckConfOption) {
	for _, o := range opts {
		o(&c.HealthCheck)
	}
}

func (c *Conf) sqsInit(opts ...sqsConfOption) {
	for _, o := range opts {
		o(&c.SQS)
	}
}

// Init confのデフォルト値はここで埋める
func (c *Conf) Init() {
	c.workerInit(intervalSec(1), maxProcessCount(1), logLevel("INFO"))
	c.healthcheckInit(maxRequestMillisec(1000))
	c.sqsInit(concurrency(1))
}

// Validate confのバリデーションはここで行う
func (c *Conf) Validate() error {
	if err := c.Worker.Validate(); err != nil {
		return err
	}

	if err := c.SQS.Validate(); err != nil {
		return err
	}

	if err := c.HealthCheck.Validate(); err != nil {
		return err
	}

	if c.Stat.ServerPort == 0 {
		return errors.New("stat.server_port is required")
	}

	return nil
}

// NewConf : confのオブジェクトを返す
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
