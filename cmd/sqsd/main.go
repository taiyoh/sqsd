package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/joho/godotenv"
	"github.com/redis/rueidis"
	"github.com/taiyoh/go-typedenv"

	sqsd "github.com/taiyoh/sqsd/v2"
	"github.com/taiyoh/sqsd/v2/locker"
	memorylocker "github.com/taiyoh/sqsd/v2/locker/memory"
	redislocker "github.com/taiyoh/sqsd/v2/locker/redis"
)

type sqsdConfig struct {
	awsConf         config.SharedConfig
	RawURL          string
	QueueURL        string
	Duration        time.Duration
	UnlockInterval  time.Duration
	LockExpire      time.Duration
	FetcherWaitTime time.Duration
	FetcherParallel int
	InvokerParallel int
	MonitoringPort  int
	LogLevel        slog.Level
	RedisLocker     *redisLocker
}

func (c sqsdConfig) withEndpoint(o *sqs.Options) {
	o.BaseEndpoint = &c.awsConf.BaseEndpoint
}

type redisLocker struct {
	Host    string
	DBName  int
	KeyName string
}

func (c *sqsdConfig) Load() error {
	if err := typedenv.Scan(
		typedenv.RequiredDirect("INVOKER_URL", &c.RawURL),
		typedenv.RequiredDirect("QUEUE_URL", &c.QueueURL),
		typedenv.DefaultDirect("INVOKER_TIMEOUT", &c.Duration, "60s"),
		typedenv.DefaultDirect("UNLOCK_INTERVAL", &c.UnlockInterval, "1m"),
		typedenv.DefaultDirect("LOCK_EXPIRE", &c.LockExpire, "24h"),
		typedenv.DefaultDirect("FETCHER_WAIT_TIME", &c.FetcherWaitTime, "1s"),
		typedenv.DefaultDirect("FETCHER_PARALLEL_COUNT", &c.FetcherParallel, "1"),
		typedenv.DefaultDirect("INVOKER_PARALLEL_COUNT", &c.InvokerParallel, "1"),
		typedenv.DefaultDirect("MONITORING_PORT", &c.MonitoringPort, "6969"),
		typedenv.Default("LOG_LEVEL", &c.LogLevel, "info"),
		typedenv.DefaultDirect("AWS_REGION", &c.awsConf.Region, "ap-northeast-1"),
		typedenv.LookupDirect("SQS_ENDPOINT_URL", &c.awsConf.BaseEndpoint),
	); err != nil {
		return err
	}

	var rl redisLocker
	if err := typedenv.Scan(
		typedenv.RequiredDirect("REDIS_LOCKER_HOST", &rl.Host),
		typedenv.DefaultDirect("REDIS_LOCKER_DBNAME", &rl.DBName, "0"),
		typedenv.RequiredDirect("REDIS_LOCKER_KEYNAME", &rl.KeyName),
	); err == nil {
		c.RedisLocker = &rl
	}

	return nil
}

func main() {
	loadEnvFromFile()

	var args sqsdConfig
	if err := args.Load(); err != nil {
		log.Fatal(err)
	}

	slogHandlerOpts := slog.HandlerOptions{Level: args.LogLevel}
	sqsd.SetWithHandlerOptions(slogHandlerOpts)

	logger := sqsd.NewLogger(slogHandlerOpts, os.Stderr, "sqsd-main")

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(args.awsConf.Region))
	if err != nil {
		log.Fatal(err)
	}

	queue := sqs.NewFromConfig(cfg, args.withEndpoint)

	var queueLocker locker.QueueLocker
	if rl := args.RedisLocker; rl != nil {
		db, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress: []string{rl.Host},
			SelectDB:    rl.DBName,
		})
		if err != nil {
			log.Fatal(err)
		}
		queueLocker = redislocker.New(db, rl.KeyName)
		logger.Info("redis queue locker is selected")
	} else {
		queueLocker = memorylocker.New()
		logger.Info("memory queue locker is selected")
	}

	unlocker, err := locker.NewUnlocker(queueLocker, args.UnlockInterval, locker.ExpireDuration(args.LockExpire))
	if err != nil {
		log.Fatal(err)
	}

	ivk, err := sqsd.NewHTTPInvoker(args.RawURL, args.Duration)
	if err != nil {
		log.Fatal(err)
	}

	var maxMessages int32 = 1

	sys := sqsd.NewSystem(
		sqsd.GatewayBuilder(queue, args.QueueURL, args.FetcherParallel, args.Duration,
			sqsd.FetcherMaxMessages(maxMessages),
			sqsd.FetcherWaitTime(args.FetcherWaitTime),
			sqsd.FetcherQueueLocker(queueLocker)),
		sqsd.ConsumerBuilder(ivk, args.InvokerParallel),
		sqsd.MonitorBuilder(args.MonitoringPort),
	)

	logger.Info("start process")
	logger.Info("queue settings", "url", args.QueueURL, "parallel", args.FetcherParallel, "wait_time", args.FetcherWaitTime.String(), "max_messages", maxMessages)
	logger.Info("invoker settings", "url", args.RawURL, "parallel", args.InvokerParallel, "timeout", args.Duration.String())

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()

	go unlocker.Run(ctx)

	if err := sys.Run(ctx); err != nil {
		log.Fatal(err)
	}

	logger.Info("end process")
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
