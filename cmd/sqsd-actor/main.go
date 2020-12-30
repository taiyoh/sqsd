package main

import (
	"flag"
	"fmt"
	plog "log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/mailbox"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/joho/godotenv"
	sqsd "github.com/taiyoh/sqsd/actor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type args struct {
	rawURL          string
	queueURL        string
	dur             time.Duration
	fetcherParallel int
	invokerParallel int
	monitoringPort  int
	logLevel        log.Level
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

	as := actor.NewActorSystem()

	ivk, err := sqsd.NewHTTPInvoker(args.rawURL, args.dur)
	if err != nil {
		plog.Fatal(err)
	}

	gw := sqsd.NewGateway(queue, args.queueURL, args.fetcherParallel)

	fetcher := as.Root.Spawn(gw.NewFetcherGroup())
	remover := as.Root.Spawn(gw.NewRemoverGroup().
		WithMailbox(mailbox.Bounded(args.fetcherParallel * 100)))

	c := sqsd.NewConsumer(ivk, remover, args.fetcherParallel)
	consumer := as.Root.Spawn(c.NewQueueActorProps().
		WithMailbox(mailbox.Bounded(args.fetcherParallel + 10)))
	monitor := as.Root.Spawn(c.NewMonitorActorProps())

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", args.monitoringPort))
	if err != nil {
		plog.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	sqsd.RegisterMonitoringServiceServer(grpcServer, sqsd.NewMonitoringService(as.Root, monitor))
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	logger := log.New(args.logLevel, "[sqsd-main]")

	logger.Info("start process")
	logger.Info("queue settings",
		log.String("url", args.queueURL),
		log.Int("parallel", args.fetcherParallel))
	logger.Info("invoker settings",
		log.String("url", args.rawURL),
		log.Int("parallel", args.invokerParallel),
		log.Duration("timeout", args.dur))

	as.Root.Send(fetcher, &sqsd.StartGateway{
		Sender: consumer,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("gRPC server start.", log.Object("addr", lis.Addr()))
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			plog.Fatal(err)
		}
		logger.Info("gRPC server closed.")
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	logger.Info("signal caught. stopping worker...", log.Object("signal", sig))
	grpcServer.Stop()

	as.Root.Stop(fetcher)

	for {
		res, err := as.Root.RequestFuture(monitor, &sqsd.CurrentWorkingsMessage{}, -1).Result()
		if err != nil {
			plog.Fatalf("failed to retrieve current_workings: %v", err)
		}
		if tasks := res.([]*sqsd.Task); len(tasks) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	wg.Wait()

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

	levelMap := map[string]log.Level{
		"debug": log.DebugLevel,
		"info":  log.InfoLevel,
		"error": log.ErrorLevel,
	}
	l := log.InfoLevel
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
	flag.StringVar(&env, "f", "", "envfile path")
	flag.Parse()

	if env == "" {
		return
	}
	if err := godotenv.Load(filepath.Join(cwd, env)); err != nil {
		plog.Fatal(err)
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
