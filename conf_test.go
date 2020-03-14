package sqsd_test

import (
	"os"
	"testing"

	"github.com/taiyoh/sqsd"
)

func TestValidateConf(t *testing.T) {
	c := &sqsd.Conf{}
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
	os.Unsetenv("WORKER_DESTINATION_URL")
	os.Setenv("AWS_ACCESS_KEY_ID", "xxxxxxxx")
	os.Setenv("SQS_QUEUE_NAME", "foobar")
	defer func() {
		os.Unsetenv("WORKER_DESTINATION_URL")
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("SQS_QUEUE_NAME")
	}()

	for _, tt := range []struct {
		label string
		url   string
	}{
		{
			label: "no worker_url defined",
			url:   "",
		},
		{
			label: "invalid url",
			url:   "fofo://bababa!ofoffo",
		},
	} {
		t.Run(tt.label, func(t *testing.T) {
			defer os.Unsetenv("WORKER_DESTINATION_URL")
			os.Setenv("WORKER_DESTINATION_URL", tt.url)
			if _, err := sqsd.NewConf(); err == nil {
				t.Fatal("error not exists")
			}
		})
	}
	t.Run("valid url", func(t *testing.T) {
		defer func() {
			os.Unsetenv("WORKER_DESTINATION_URL")
		}()
		u := "http://example.com/worker"
		os.Setenv("WORKER_DESTINATION_URL", u)
		conf, err := sqsd.NewConf()
		if err != nil {
			t.Fatal(err)
		}
		if conf.Worker.URL != u {
			t.Errorf("expected url: %s, actual: %s", u, conf.Worker.URL)
		}
		if qu := conf.SQS.QueueURL(); qu != "https://sqs.us-east-1.amazonaws.com/xxxxxxxx/foobar" {
			t.Errorf("expected: https://sqs.us-east-1.amazonaws.com/xxxxxxxx/foobar, actual: %s", qu)
		}
	})
}
