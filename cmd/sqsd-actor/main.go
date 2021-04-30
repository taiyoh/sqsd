package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	LogLevel        sqsd.LogLevel `env:"LOG_LEVEL" envDefault:"info"`
}

func main() {
	loadEnvFromFile()

	args := config{}
	if err := env.Parse(&args); err != nil {
		log.Fatal(err)
	}

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

	sys := sqsd.NewSystem(
		sqsd.GatewayBuilder(queue, args.QueueURL, args.FetcherParallel, args.Duration),
		sqsd.ConsumerBuilder(ivk, args.InvokerParallel),
		sqsd.MonitorBuilder(args.MonitoringPort),
	)

	logger := palog.New(args.LogLevel.Level, "[sqsd-main]")

	logger.Info("start process")
	logger.Info("queue settings",
		palog.String("url", args.QueueURL),
		palog.Int("parallel", args.FetcherParallel))
	logger.Info("invoker settings",
		palog.String("url", args.RawURL),
		palog.Int("parallel", args.InvokerParallel),
		palog.Duration("timeout", args.Duration))

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()

	if err := sys.Run(ctx); err != nil {
		log.Fatal(err)
	}

	logger.Info("end process")

	time.Sleep(500 * time.Millisecond)
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
