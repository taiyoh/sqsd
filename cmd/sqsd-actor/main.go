package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	palog "github.com/AsynkronIT/protoactor-go/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
	sqsd "github.com/taiyoh/sqsd/actor"
)

type config struct {
	RawURL          string        `env:"INVOKER_URL,required"`
	QueueURL        string        `env:"QUEUE_URL,required"`
	Duration        time.Duration `env:"DEFAULT_INVOKER_TIMEOUT" envDefault:"60s"`
	FetcherParallel int           `env:"FETCHER_PARALLEL_COUNT" envDefault:"1"`
	InvokerParallel int           `env:"INVOKER_PARALLEL_COUNT" envDefault:"1"`
	MonitoringPort  int           `env:"MONITORING_PORT" envDefault:"6969"`
	LogLevel        palog.Level   `env:"LOG_LEVEL" envDefault:"info"`
}

func main() {
	loadEnvFromFile()

	args := parse()
	sqsd.SetLogLevel(args.LogLevel)

	queue := sqs.New(
		session.Must(session.NewSession()),
		aws.NewConfig().
			WithEndpoint(os.Getenv("SQS_ENDPOINT_URL")).
			WithRegion(os.Getenv("AWS_REGION")),
	)

	ivk, err := sqsd.NewHTTPInvoker(args.RawURL, args.Duration)
	if err != nil {
		log.Fatal(err)
	}

	sys := sqsd.NewSystem(queue, ivk, sqsd.SystemConfig{
		QueueURL:          args.QueueURL,
		FetcherParallel:   args.FetcherParallel,
		InvokerParallel:   args.InvokerParallel,
		VisibilityTimeout: args.Duration,
		MonitoringPort:    args.MonitoringPort,
	})

	logger := initLogger(args)

	if err := sys.Start(); err != nil {
		log.Fatal(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	logger.Info("signal caught. stopping worker...", palog.Object("signal", sig))

	if err := sys.Stop(); err != nil {
		log.Fatalf("failed to retrieve current_workings: %v", err)
	}

	logger.Info("end process")

	time.Sleep(500 * time.Millisecond)
}

var logParser env.ParserFunc = func(v string) (interface{}, error) {
	levelMap := map[string]palog.Level{
		"debug": palog.DebugLevel,
		"info":  palog.InfoLevel,
		"warn":  palog.WarnLevel,
		"error": palog.ErrorLevel,
	}
	l, ok := levelMap[v]
	if !ok {
		return 0, fmt.Errorf("invalid LOG_LEVEL: %s", v)
	}
	return l, nil
}

func parse() config {
	cfg := config{}
	funcMap := map[reflect.Type]env.ParserFunc{
		reflect.TypeOf(palog.InfoLevel): logParser,
	}
	if err := env.ParseWithFuncs(&cfg, funcMap); err != nil {
		log.Fatal(err)
	}
	return cfg
}

var cwd, _ = os.Getwd()

func loadEnvFromFile() {
	var env string
	flag.StringVar(&env, "e", "", "envfile path")
	flag.Parse()

	if env == "" {
		return
	}
	fp := filepath.Join(cwd, env)
	if _, err := os.Stat(fp); err != nil {
		fp = env
	}
	if err := godotenv.Load(fp); err != nil {
		log.Fatal(err)
	}
}

func initLogger(args config) *palog.Logger {
	logger := palog.New(args.LogLevel, "[sqsd-main]")

	logger.Info("start process")
	logger.Info("queue settings",
		palog.String("url", args.QueueURL),
		palog.Int("parallel", args.FetcherParallel))
	logger.Info("invoker settings",
		palog.String("url", args.RawURL),
		palog.Int("parallel", args.InvokerParallel),
		palog.Duration("timeout", args.Duration))

	return logger
}
