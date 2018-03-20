package sqsd

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestInitConf(t *testing.T) {
	c := &Conf{}
	if c.Worker.MaxProcessCount != 0 {
		t.Error("MaxProcessCount is invalid")
	}
	if c.Worker.IntervalSeconds != 0 {
		t.Error("IntervalSeconds is invalid")
	}
	c.Init()
	if c.Worker.MaxProcessCount != 1 {
		t.Error("MaxProcessCount is yet 0")
	}
	if c.Worker.IntervalSeconds != 1 {
		t.Error("IntervalSeconds not set!")
	}
}

func TestValidateConf(t *testing.T) {
	c := &Conf{}
	c.Init()
	c.SQS.AccountID = "foo"
	c.SQS.QueueName = "bar"
	c.SQS.Region = "ap-northeast-1"
	c.Worker.WorkerURL = "http://localhost:1080/run_job"
	c.Stat.ServerPort = 10000

	if err := c.Validate(); err != nil {
		t.Error("valid conf but error found", err)
	}

	c.SQS.Region = ""
	if err := c.Validate(); err == nil {
		t.Error("sqs.region is required but valid config")
	}

	c.Stat.ServerPort = 0
	if err := c.Validate(); err == nil {
		t.Error("stat.server_port is 0, but no error")
	}

	c.Stat.ServerPort = 10000
	c.SQS.QueueName = ""
	if err := c.Validate(); err == nil {
		t.Error("sqs.queue_name is required")
	}
	c.SQS.QueueName = "bar"
	c.SQS.AccountID = ""
	if err := c.Validate(); err == nil {
		t.Error("sqs.account_id is required")
	}
	c.SQS.AccountID = "foo"
	c.SQS.Region = ""
	if err := c.Validate(); err == nil {
		t.Error("SQS.Region not exists, but no error")
	}
	c.SQS.Region = "ap-northeast-1"

	c.SQS.URL = "foo://bar/baz"
	if err := c.Validate(); err == nil {
		t.Error("SQS.URL should be url")
	}
	c.SQS.URL = ""

	c.Worker.WorkerURL = ""
	if err := c.Validate(); err == nil {
		t.Error("Worker.WorkerURL is empty, but no error")
	}
	c.Worker.WorkerURL = "foo://bar/baz"
	if err := c.Validate(); err == nil {
		t.Error("Worker.WorkerURL is not HTTP url, but no error")
	}
	c.Worker.WorkerURL = "http://localhost/foo/bar"
	c.Worker.LogLevel = "WRONG"
	if err := c.Validate(); err == nil {
		t.Error("Worker.LogLevel should be invalid")
	}
	c.Worker.LogLevel = "INFO"
	c.Worker.HealthCheckURL = "hoge://fuga/piyo"
	if err := c.Validate(); err == nil {
		t.Error("Worker.HealthCheckURL should be invalid")
	}
	c.Worker.HealthCheckURL = "http://localhost/hoge/fuga"
	if err := c.Validate(); err != nil {
		t.Error("WorkerConf should be valid: ", err)
	}
}

func TestNewConf(t *testing.T) {
	d, _ := os.Getwd()
	if _, err := NewConf(filepath.Join(d, "test", "conf", "hoge.toml")); err == nil {
		t.Error("file not found")
	}
	if _, err := NewConf(filepath.Join(d, "test", "conf", "config1.toml")); err == nil {
		t.Error("invalid config but passed")
	}
	conf, err := NewConf(filepath.Join(d, "test", "conf", "config_valid.toml"))
	if err != nil {
		t.Error("invalid config??? ", err)
	}
	if conf.SQS.QueueURL() != "https://sqs.ap-northeast-1.amazonaws.com/foobar/hoge" {
		t.Error("QueueURL not loaded correctly. " + conf.SQS.QueueURL())
	}
	if conf.Stat.ServerPort != 4080 {
		t.Error("Stat.ServerPort not loaded correctly. " + strconv.Itoa(conf.Stat.ServerPort))
	}

	conf.SQS.URL = "http://localhost:4649/foo/bar"
	if conf.SQS.QueueURL() != "http://localhost:4649/foo/bar" {
		t.Error("SQS.URL has priority than building url." + conf.SQS.QueueURL())
	}
}
