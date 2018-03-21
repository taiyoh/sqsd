package sqsd

import (
	"errors"
	"net/url"
	"strings"

	"github.com/pelletier/go-toml"
)

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

func (c WorkerConf) Validate() error {
	uri, err := url.ParseRequestURI(c.WorkerURL)
	if err != nil || !strings.HasPrefix(uri.Scheme, "http") {
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
	AccountID string `toml:"account_id"`
	QueueName string `toml:"queue_name"`
	Region    string `toml:"region"`
	URL       string `toml:"url"`
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
		uri, err := url.ParseRequestURI(c.URL)
		if err != nil || !strings.HasPrefix(uri.Scheme, "http") {
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
		uri, err := url.ParseRequestURI(c.URL)
		if err != nil || !strings.HasPrefix(uri.Scheme, "http") {
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

// Init confのデフォルト値はここで埋める
func (c *Conf) Init() {
	if c.Worker.IntervalSeconds == 0 {
		c.Worker.IntervalSeconds = 1
	}
	if c.Worker.MaxProcessCount == 0 {
		c.Worker.MaxProcessCount = 1
	}
	if c.Worker.LogLevel == "" {
		c.Worker.LogLevel = "INFO"
	}

	if c.HealthCheck.URL != "" && c.HealthCheck.MaxRequestMS == 0 {
		c.HealthCheck.MaxRequestMS = 1000
	}
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
