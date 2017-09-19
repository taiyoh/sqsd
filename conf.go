package sqsd

import (
	"errors"
	"fmt"
	"github.com/pelletier/go-toml"
	"net/url"
	"strings"
)

type Conf struct {
	QueueURL              string         `toml:"queue_url"`
	HTTPWorker            HttpWorkerConf `toml:"http_worker"`
	Stat                  StatConf       `toml:"stat"`
	MaxMessagesPerRequest int64          `toml:"max_message_per_request"`
	SleepSeconds          int64          `toml:"sleep_seconds"`
	WaitTimeSeconds       int64          `toml:"wait_time_seconds"`
	ProcessCount          int            `toml:"process_count"`
}

type HttpWorkerConf struct {
	URL                string `toml:"url"`
	RequestContentType string `toml:"request_content_type"`
}

type StatConf struct {
	Port int `toml:"port"`
}

// Init : confのデフォルト値はここで埋める
func (c *Conf) Init() {
	if c.HTTPWorker.RequestContentType == "" {
		c.HTTPWorker.RequestContentType = "application/json"
	}
	if c.MaxMessagesPerRequest == 0 {
		c.MaxMessagesPerRequest = 1
	}
}

// Validate : confのバリデーションはここで行う
func (c *Conf) Validate() error {
	if c.MaxMessagesPerRequest > 10 || c.MaxMessagesPerRequest < 1 {
		return errors.New("MaxMessagesPerRequest limit is 10")
	}

	if c.WaitTimeSeconds > 20 || c.WaitTimeSeconds < 0 {
		return errors.New("WaitTimeSeconds range: 0 - 20")
	}

	if c.SleepSeconds < 0 {
		return errors.New("SleepSeconds requires natural number")
	}
	uri, err := url.ParseRequestURI(c.QueueURL)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(uri.Scheme, "http") {
		return errors.New("QueueURL is not URL")
	}
	return nil
}

// NewConf : confのオブジェクトを返す
func NewConf(filepath string) (*Conf, error) {
	config, err := toml.LoadFile(filepath)
	if err != nil {
		fmt.Println("filepath: " + filepath)
		fmt.Println("Error ", err.Error())
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
