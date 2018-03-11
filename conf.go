package sqsd

import (
	"errors"
	"log"
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
	LogLevel        string `toml:"log_level"`
}

func (c WorkerConf) Validate() error {
	uri, err := url.ParseRequestURI(c.JobURL)
	if err != nil || !strings.HasPrefix(uri.Scheme, "http") {
		return errors.New("worker.job_url is not HTTP URL: " + c.JobURL)
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
}

func (c SQSConf) Validate() error {
	if c.AccountID == "" {
		return errors.New("sqs.account_id is required")
	}
	if c.QueueName == "" {
		return errors.New("sqs.queue_name is required")
	}
	if c.Region == "" {
		return errors.New("sqs.region is required")
	}
	return nil
}

func (c SQSConf) QueueURL() string {
	return "https://sqs." + c.Region + ".amazonaws.com/" + c.AccountID + "/" + c.QueueName
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
}

// Validate confのバリデーションはここで行う
func (c *Conf) Validate() error {
	if err := c.Worker.Validate(); err != nil {
		return err
	}

	if err := c.SQS.Validate(); err != nil {
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
		log.Println("bbbbbbbbbb", err)
		return nil, err
	}

	log.Println("cccccccc")
	return sqsdConf, nil
}
