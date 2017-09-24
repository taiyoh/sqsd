package sqsd

import (
	"errors"
	"net/url"
	"strings"

	"github.com/pelletier/go-toml"
)

type Conf struct {
	Worker WorkerConf `toml:"worker"`
	Stat   StatConf   `toml:"stat"`
	SQS    SQSConf    `toml:"sqs"`
}

type WorkerConf struct {
	IntervalSeconds uint64 `toml:"interval_seconds"`
	MaxProcessCount uint   `toml:"max_process_count"`
	JobURL          string `toml:"job_url"`
}

type StatConf struct {
	ServerPort int `toml:"server_port"`
}

type SQSConf struct {
	QueueURL string `toml:"queue_url"`
	Region   string `toml:"region"`
}

// Init confのデフォルト値はここで埋める
func (c *Conf) Init() {
	if c.Worker.IntervalSeconds == 0 {
		c.Worker.IntervalSeconds = 1
	}
	if c.Worker.MaxProcessCount == 0 {
		c.Worker.MaxProcessCount = 1
	}
}

// Validate confのバリデーションはここで行う
func (c *Conf) Validate() error {
	for k, urlstr := range map[string]string{
		"sqs.queue_url":  c.SQS.QueueURL,
		"worker.job_url": c.Worker.JobURL,
	} {
		uri, err := url.ParseRequestURI(urlstr)
		if err != nil || !strings.HasPrefix(uri.Scheme, "http") {
			return errors.New(k + " is not HTTP URL: " + urlstr)
		}
	}

	if c.SQS.Region == "" {
		return errors.New("sqs.region is required")
	}

	if c.Stat.ServerPort == 0 {
		return errors.New("stat.server_port is required")
	}

	if c.SQS.Region == "" {
		return errors.New("sqs.region is required")
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
