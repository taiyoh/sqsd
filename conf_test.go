package sqsd

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestValidateConf(t *testing.T) {
	c := &Conf{}
	c.Init()
	c.SQS.AccountID = "foo"
	c.SQS.QueueName = "bar"
	c.SQS.Region = "ap-northeast-1"
	c.Worker.URL = "http://localhost:1080/run_job"
	c.Main.StatServerPort = 4000

	if err := c.Validate(); err != nil {
		t.Error("valid conf but error found", err)
	}

	c.SQS.Region = ""
	if err := c.Validate(); err == nil {
		t.Error("sqs.region is required but valid config")
	}

	c.Main.StatServerPort = 0
	if err := c.Validate(); err == nil {
		t.Error("stat.server_port is 0, but no error")
	}

	c.Main.StatServerPort = 10000
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

	c.Worker.URL = ""
	if err := c.Validate(); err == nil {
		t.Error("Worker.WorkerURL is empty, but no error")
	}
	c.Worker.URL = "foo://bar/baz"
	if err := c.Validate(); err == nil {
		t.Error("Worker.WorkerURL is not HTTP url, but no error")
	}
	c.Worker.URL = "http://localhost/foo/bar"
	c.Main.LogLevel = "WRONG"
	if err := c.Validate(); err == nil {
		t.Error("Worker.LogLevel should be invalid")
	}
	c.Main.LogLevel = "INFO"

	if c.Worker.Healthcheck.ShouldSupport() {
		t.Error("healthcheck should not support for empty url")
	}

	c.Worker.Healthcheck.URL = "hoge://fuga/piyo"
	if err := c.Validate(); err == nil {
		t.Error("HealthCheck.URL should be invalid")
	}
	c.Worker.Healthcheck.URL = "http://localhost/hoge/fuga"
	if err := c.Validate(); err == nil {
		t.Error("HealthcheckMaxElapsedSec is required")
	}
	c.Worker.Healthcheck.MaxElapsedSec = 1

	if !c.Worker.Healthcheck.ShouldSupport() {
		t.Error("healthcheck should support for filled url")
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
	if conf.Main.StatServerPort != 4080 {
		t.Error("Main.StatServerPort not loaded correctly. " + strconv.Itoa(conf.Main.StatServerPort))
	}

	conf.SQS.URL = "http://localhost:4649/foo/bar"
	if conf.SQS.QueueURL() != "http://localhost:4649/foo/bar" {
		t.Error("SQS.URL has priority than building url." + conf.SQS.QueueURL())
	}

	if conf.SQS.Concurrency != 5 {
		t.Error("SQS.Concurrency should be 5")
	}
}
