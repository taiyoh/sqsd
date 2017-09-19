package sqsd

import "testing"
import "os"
import "path/filepath"
import "strconv"

func TestInitConf(t *testing.T) {
	c := &Conf{}
	if c.MaxMessagesPerRequest != 0 {
		t.Error("MaxMessagesPerRequest not 0")
	}
	if c.HTTPWorker.RequestContentType != "" {
		t.Error("contentType is already set!")
	}
	c.Init()
	if c.MaxMessagesPerRequest != 1 {
		t.Error("MaxMessagesPerRequest is yet 0")
	}
	if c.HTTPWorker.RequestContentType != "application/json" {
		t.Error("contentType is not set!")
	}
}

func TestValidateConf(t *testing.T) {
	c := &Conf{}
	c.QueueURL = "https://example.com/queue/hoge"
	c.MaxMessagesPerRequest = 11
	if err := c.Validate(); err == nil {
		t.Error("MaxMessagesPerRequest = 11")
	}
	c.MaxMessagesPerRequest = 10
	if err := c.Validate(); err != nil {
		t.Error("MaxMessagesPerRequest = 10")
	}
	c.MaxMessagesPerRequest = 1
	if err := c.Validate(); err != nil {
		t.Error("MaxMessagesPerRequest = 1")
	}
	c.MaxMessagesPerRequest = 0
	if err := c.Validate(); err == nil {
		t.Error("MaxMessagesPerRequest = 0")
	}
	c.MaxMessagesPerRequest = 10
	c.WaitTimeSeconds = 21
	if err := c.Validate(); err == nil {
		t.Error("WaitTimeSeconds = 21")
	}
	c.WaitTimeSeconds = 20
	if err := c.Validate(); err != nil {
		t.Error("WaitTimeSeconds = 20")
	}
	c.WaitTimeSeconds = 0
	if err := c.Validate(); err != nil {
		t.Error("WaitTimeSeconds = 0")
	}
	c.WaitTimeSeconds = -1
	if err := c.Validate(); err == nil {
		t.Error("WaitTimeSeconds = -1")
	}
	c.WaitTimeSeconds = 0
	c.SleepSeconds = 1
	if err := c.Validate(); err != nil {
		t.Error("SleepSeconds = 1")
	}
	c.SleepSeconds = 0
	if err := c.Validate(); err != nil {
		t.Error("SleepSeconds = 0")
	}
	c.SleepSeconds = -1
	if err := c.Validate(); err == nil {
		t.Error("SleepSeconds = -1")
	}

	c.SleepSeconds = 1
	c.QueueURL = ""
	if err := c.Validate(); err == nil {
		t.Error("QueueURL not found")
	}
	c.QueueURL = "hoge://fuga/piyo"
	if err := c.Validate(); err == nil {
		t.Error("QueueURL is invalid")
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
		t.Error("invalid config??? " + err.Error())
	}
	if conf.QueueURL != "http://example.com/queue/hoge" {
		t.Error("QueueURL not loaded correctly. " + conf.QueueURL)
	}
	if conf.Stat.Port != 4080 {
		t.Error("Stat.Port not loaded correctly. " + strconv.Itoa(conf.Stat.Port))
	}
}
