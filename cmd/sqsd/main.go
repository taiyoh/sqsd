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
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	sqsd "github.com/taiyoh/sqsd"
	"github.com/taiyoh/sqsd/locker"
	memorylocker "github.com/taiyoh/sqsd/locker/memory"
	redislocker "github.com/taiyoh/sqsd/locker/redis"
)

type config struct {
	RawURL          string        `env:"INVOKER_URL,required"`
	QueueURL        string        `env:"QUEUE_URL,required"`
	Duration        time.Duration `env:"INVOKER_TIMEOUT" envDefault:"60s"`
	UnlockInterval  time.Duration `env:"UNLOCK_INTERVAL" envDefault:"1m"`
	LockExpire      time.Duration `env:"LOCK_EXPIRE" envDefault:"24h"`
	FetcherParallel int           `env:"FETCHER_PARALLEL_COUNT" envDefault:"1"`
	InvokerParallel int           `env:"INVOKER_PARALLEL_COUNT" envDefault:"1"`
	MonitoringPort  int           `env:"MONITORING_PORT" envDefault:"6969"`
	LogLevel        sqsd.LogLevel `env:"LOG_LEVEL" envDefault:"info"`
	RedisLocker     *redisLocker  `env:"-"`
}

type redisLocker struct {
	Host    string `env:"HOST,required"`
	DBName  int    `env:"DBNAME" envDefault:"0"`
	KeyName string `env:"KEYNAME,required"`
}

func (c *config) Load() error {
	if err := env.Parse(c); err != nil {
		return err
	}
	var rl redisLocker
	if err := env.Parse(&rl, env.Options{
		Prefix: "REDIS_LOCKER_",
	}); err == nil {
		c.RedisLocker = &rl
	}
	return nil
}

func main() {
	loadEnvFromFile()

	var args config
	if err := args.Load(); err != nil {
		log.Fatal(err)
	}

	sqsd.SetLogLevel(args.LogLevel)

	logger := palog.New(args.LogLevel.Level, "[sqsd-main]")

	queue := sqs.New(
		session.Must(session.NewSession()),
		aws.NewConfig().
			WithEndpoint(os.Getenv("SQS_ENDPOINT_URL")).
			WithRegion(os.Getenv("AWS_REGION")),
	)

	var queueLocker locker.QueueLocker
	if rl := args.RedisLocker; rl != nil {
		db := redis.NewClient(&redis.Options{
			Addr: rl.Host,
			DB:   rl.DBName,
		})
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

	sys := sqsd.NewSystem(
		sqsd.GatewayBuilder(queue, args.QueueURL, args.FetcherParallel, args.Duration,
			sqsd.FetcherQueueLocker(queueLocker)),
		sqsd.ConsumerBuilder(ivk, args.InvokerParallel),
		sqsd.MonitorBuilder(args.MonitoringPort),
	)

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

	go unlocker.Run(ctx)

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
