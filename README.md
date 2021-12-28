# sqsd by golang

## Motivation

This tool emulates [sqsd of AWS Elastic Beanstalk worker environments](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/using-features-managing-env-tiers.html).

The concept of sqsd is simplifying worker process management by lightwaight languages such as Perl and PHP,  
and separating consuming process from SQS and job process.  
These languages has no defact standard worker libary such as Ruby's `sidekiq`,  
so it's difficult to build worker system or to manage it reliablly.

`sqsd` works only two things:

- Fetching queue message from SQS
    - also removing it when job is succeeded
- Invoking message to job process by **HTTP POST request**

Many languages' HTTP server library are stable, so user builds `worker server` by HTTP server.

This (github.com/taiyoh/sqsd) builds its concept without AWS Elastic Beanstalk.

## Features

- designed by Golang
    - fast and low memory usage
- based on [protoactor-go](https://github.com/asynkron/protoactor-go)
    - [actor model](https://en.wikipedia.org/wiki/Actor_model)
    - clearing internal component responsibility
- fetch scoreboard by gRPC
- run circuit breaker if all worker processes are busy
    - stops automatically if prosecces are not busy
    - unlike CSP, gently run and stop switching by actor model
- invoke job function directly
    - accepts `sqsd.Invoker` interface only

## Usage

### as single binary

setup .env file

```shell
INVOKER_URL=http://local.example.com/setup/your/worker/path
QUEUE_URL=https://queue.amazonaws.com/80398EXAMPLE/MyQueue
# INVOKER_TIMEOUT=60s # default
# UNLOCK_INTERVAL=1m # default
# LOCK_EXPIRE=24h # default
# FETCHER_PARALLEL_COUNT=1 # default
# INVOKER_PARALLEL_COUNT=1 # default
# MONITORING_PORT=6969 # default
# LOG_LEVEL=info # default
```

run it

```shell
$ sqsd -e .env
```

or

```shell
$ source .env
$ sqsd
```

NOTE: sqsd single binary supports HTTP invocation only.

### as library

```go
type myInvoker struct{}

func (myInvoker) Invoke(ctx context.Context, q sqsd.Message) error {
    // here is your job process
	return nil
}

func main() {
	sqsd.SetLogLevel(os.Getenv("LOG_LEVEL"))

	queue := sqs.New(session.Must(session.NewSession()))

    dur, _ := time.ParseDuration(os.Getenv("DEFAULT_INVOKER_TIMEOUT"))
    port, _ := strconv.ParseInt(os.Getenv("MONITORING_PORT"), 10, 64)
    fetcherParallel, _ := strconv.ParseInt(os.Getenv("FETCHER_PARALLEL_COUNT"), 10, 64)
    invokerParallel, _ := strconv.ParseInt(os.Getenv("INVOKER_PARALLEL_COUNT"), 10, 64)

    var ivk myInvoker

	sys := sqsd.NewSystem(
		sqsd.GatewayBuilder(queue, os.Getenv("QUEUE_URL"), int(fetcherParallel), dur),
		sqsd.ConsumerBuilder(ivk, int(invokerParallel)),
		sqsd.MonitorBuilder(int(port)),
	)

	logger := protoactorlog.New(protoactorlog.InfoLevel, "[your-worker]")

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()

	if err := sys.Run(ctx); err != nil {
		panic(err)
	}

	logger.Info("end process")

    // wait until log buffer is flushed
	time.Sleep(500 * time.Millisecond)
}
```
