package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	palog "github.com/AsynkronIT/protoactor-go/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/joho/godotenv"
	sqsd "github.com/taiyoh/sqsd/actor"
)

type args struct {
	rawURL          string
	queueURL        string
	dur             time.Duration
	fetcherParallel int
	invokerParallel int
	monitoringPort  int
	logLevel        palog.Level
}

func main() {
	loadEnvFromFile()

	args := parse()
	sqsd.SetLogLevel(args.logLevel)

	queue := sqs.New(
		session.Must(session.NewSession()),
		aws.NewConfig().
			WithEndpoint(os.Getenv("SQS_ENDPOINT_URL")).
			WithRegion(os.Getenv("AWS_REGION")),
	)

	ivk, err := sqsd.NewHTTPInvoker(args.rawURL, args.dur)
	if err != nil {
		log.Fatal(err)
	}

	sys := sqsd.NewSystem(queue, ivk, sqsd.SystemConfig{
		QueueURL:          args.queueURL,
		FetcherParallel:   args.fetcherParallel,
		InvokerParallel:   args.invokerParallel,
		VisibilityTimeout: args.dur,
		MonitoringPort:    args.monitoringPort,
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

func parse() args {
	rawURL := mustGetenv("INVOKER_URL")
	queueURL := mustGetenv("QUEUE_URL")
	defaultTimeOutSeconds := defaultIntGetEnv("DEFAULT_INVOKER_TIMEOUT_SECONDS", 60)
	fetcherParallel := defaultIntGetEnv("FETCHER_PARALLEL_COUNT", 1)
	invokerParallel := defaultIntGetEnv("INVOKER_PARALLEL_COUNT", 1)
	monitoringPort := defaultIntGetEnv("MONITORING_PORT", 6969)

	levelMap := map[string]palog.Level{
		"debug": palog.DebugLevel,
		"info":  palog.InfoLevel,
		"error": palog.ErrorLevel,
	}
	l := palog.InfoLevel
	if ll, ok := os.LookupEnv("LOG_LEVEL"); ok {
		lll, ok := levelMap[ll]
		if !ok {
			panic("invalid LOG_LEVEL")
		}
		l = lll
	}

	return args{
		rawURL:          rawURL,
		queueURL:        queueURL,
		dur:             time.Duration(defaultTimeOutSeconds) * time.Second,
		fetcherParallel: fetcherParallel,
		invokerParallel: invokerParallel,
		monitoringPort:  monitoringPort,
		logLevel:        l,
	}
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

func mustGetenv(key string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	panic(key + " is required")
}

func defaultIntGetEnv(key string, defaultVal int) int {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return defaultVal
	}
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		panic(err)
	}
	return int(i)
}

func initLogger(args args) *palog.Logger {
	logger := palog.New(args.logLevel, "[sqsd-main]")

	logger.Info("start process")
	logger.Info("queue settings",
		palog.String("url", args.queueURL),
		palog.Int("parallel", args.fetcherParallel))
	logger.Info("invoker settings",
		palog.String("url", args.rawURL),
		palog.Int("parallel", args.invokerParallel),
		palog.Duration("timeout", args.dur))

	return logger
}
