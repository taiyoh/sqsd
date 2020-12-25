package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/mailbox"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	sqsd "github.com/taiyoh/sqsd/actor"
)

type args struct {
	rawURL   string
	queueURL string
	dur      time.Duration
	parallel int
}

func main() {
	as := actor.NewActorSystem()

	args := parse()

	ivk, err := sqsd.NewHTTPInvoker(args.rawURL, args.dur)
	if err != nil {
		log.Fatal(err)
	}

	queue := sqs.New(session.Must(session.NewSession()))

	f := sqsd.NewFetcher(queue, args.queueURL, args.parallel)
	r := sqsd.NewRemover(queue, args.queueURL, args.parallel)

	fetcher := as.Root.Spawn(f.NewBroadcastPool())
	remover := as.Root.Spawn(r.NewRoundRobinGroup().
		WithMailbox(mailbox.Bounded(args.parallel * 100)))

	c := sqsd.NewConsumer(ivk, remover, args.parallel)
	consumer := as.Root.Spawn(c.NewQueueActorProps().
		WithMailbox(mailbox.Bounded(args.parallel + 10)))
	monitor := as.Root.Spawn(c.NewMonitorActorProps())

	as.Root.Send(fetcher, &sqsd.StartGateway{
		Sender: consumer,
	})

	var wg sync.WaitGroup

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGINT)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sigCh
		as.Root.Send(fetcher, &sqsd.StopGateway{})
		for {
			res, err := as.Root.RequestFuture(monitor, &sqsd.CurrentWorkingsMessage{}, -1).Result()
			if err != nil {
				log.Fatal(err)
			}
			if tasks := res.([]*sqsd.Task); len(tasks) == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	wg.Wait()
}

func parse() args {
	rawURL := mustGetenv("INVOKER_URL")
	queueURL := mustGetenv("QUEUE_URL")
	defaultTimeOutSeconds := defaultIntGetEnv("DEFAULT_INVOKER_TIMEOUT_SECONDS", 60)
	parallel := defaultIntGetEnv("PARALLEL_COUNT", 1)

	return args{
		rawURL:   rawURL,
		queueURL: queueURL,
		dur:      time.Duration(defaultTimeOutSeconds) * time.Second,
		parallel: parallel,
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
