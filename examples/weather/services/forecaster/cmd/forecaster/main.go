package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"goa.design/clue/clue"
	"goa.design/clue/debug"
	"goa.design/clue/health"
	"goa.design/clue/log"
	goahttp "goa.design/goa/v3/http"
	"google.golang.org/grpc/credentials/insecure"

	"goa.design/pulse/examples/weather/services/forecaster"
	"goa.design/pulse/examples/weather/services/forecaster/clients/poller"
	genforecaster "goa.design/pulse/examples/weather/services/forecaster/gen/forecaster"
	genhttp "goa.design/pulse/examples/weather/services/forecaster/gen/http/forecaster/server"
)

func main() {
	var (
		httpaddr    = flag.String("http-addr", ":8080", "HTTP listen address")
		metricsAddr = flag.String("metrics-addr", ":8085", "metrics listen address")
		polleraddr  = flag.String("poller-addr", ":8082", "Poller service HTTP address")
		redisurl    = flag.String("redis-url", "redis://default:"+os.Getenv("REDIS_PASSWORD")+"@localhost:6379/0", "Redis URL")
		coladdr     = flag.String("otel-addr", ":4317", "OpenTelemetry collector listen address")
		debugf      = flag.Bool("debug", false, "Enable debug logs")
	)
	flag.Parse()

	// 1. Create logger
	format := log.FormatJSON
	if log.IsTerminal() {
		format = log.FormatTerminal
	}
	ctx := log.Context(context.Background(), log.WithFormat(format), log.WithFunc(log.Span))
	ctx = log.With(ctx, log.KV{K: "svc", V: genforecaster.ServiceName})
	if *debugf {
		ctx = log.Context(ctx, log.WithDebug())
		log.Debugf(ctx, "debug logs enabled")
	}

	// 2. Setup instrumentation
	spanExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(*coladdr),
		otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf(ctx, err, "failed to initialize tracing")
	}
	defer func() {
		// Create new context in case the parent context has been canceled.
		ctx := log.Context(context.Background(), log.WithFormat(format))
		if err := spanExporter.Shutdown(ctx); err != nil {
			log.Errorf(ctx, err, "failed to shutdown tracing")
		}
	}()
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(*coladdr),
		otlpmetricgrpc.WithTLSCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf(ctx, err, "failed to initialize metrics")
	}
	defer func() {
		// Create new context in case the parent context has been canceled.
		ctx := log.Context(context.Background(), log.WithFormat(format))
		if err := metricExporter.Shutdown(ctx); err != nil {
			log.Errorf(ctx, err, "failed to shutdown metrics")
		}
	}()
	cfg, err := clue.NewConfig(ctx,
		genforecaster.ServiceName,
		genforecaster.APIVersion,
		metricExporter,
		spanExporter,
	)
	if err != nil {
		log.Fatalf(ctx, err, "failed to initialize instrumentation")
	}
	clue.ConfigureOpenTelemetry(ctx, cfg)

	// 4. Create clients

	// Poller service client
	addr := *polleraddr
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		log.Errorf(ctx, err, "failed to parse poller address")
		os.Exit(1)
	}
	scheme, host := u.Scheme, u.Host
	httpc := &http.Client{
		Transport: log.Client(
			otelhttp.NewTransport(
				http.DefaultTransport,
				otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
					return otelhttptrace.NewClientTrace(ctx)
				}),
			))}
	pc := poller.New(scheme, host, httpc)

	// Pulse replicated map for forecast cache
	opt, err := redis.ParseURL(*redisurl)
	if err != nil {
		log.Errorf(ctx, err, "failed to parse Redis URL")
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)

	// 5. Mount health check & metrics on separate HTTP server (different listen port)
	check := health.Handler(health.NewChecker(
		health.NewPinger("pc", *polleraddr),
	))
	check = log.HTTP(ctx)(check).(http.HandlerFunc) // Log health-check errors
	http.Handle("/healthz", check)
	http.Handle("/livez", check)
	metricsServer := &http.Server{Addr: *metricsAddr}

	// 6. Create service & endpoints
	svc := forecaster.New(ctx, pc, rdb)
	endpoints := genforecaster.NewEndpoints(svc)
	endpoints.Use(debug.LogPayloads())
	endpoints.Use(log.Endpoint)

	// 7. Create transport
	mux := goahttp.NewMuxer()
	debug.MountDebugLogEnabler(debug.Adapt(mux))
	debug.MountPprofHandlers(debug.Adapt(mux))
	handler := otelhttp.NewHandler(mux, genforecaster.ServiceName) // 3. Add OpenTelemetry instrumentation
	handler = debug.HTTP()(handler)                                // 2. Add debug endpoints
	handler = log.HTTP(ctx)(handler)                               // 1. Add logger to request context
	server := genhttp.New(endpoints, mux, goahttp.RequestDecoder, goahttp.ResponseEncoder, nil, nil)
	genhttp.Mount(mux, server)
	for _, m := range server.Mounts {
		log.Print(ctx, log.KV{K: "method", V: m.Method}, log.KV{K: "endpoint", V: m.Verb + " " + m.Pattern})
	}
	httpServer := &http.Server{Addr: *httpaddr, Handler: handler}

	// 8. Start HTTP servers
	errc := make(chan error)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		go func() {
			log.Printf(ctx, "HTTP server listening on %s", *httpaddr)
			errc <- httpServer.ListenAndServe()
		}()

		go func() {
			log.Printf(ctx, "Metrics server listening on %s", *metricsAddr)
			errc <- metricsServer.ListenAndServe()
		}()

		<-ctx.Done()
		log.Printf(ctx, "shutting down HTTP servers")

		if err := svc.Stop(); err != nil {
			log.Errorf(ctx, err, "failed to stop service")
		}

		// Shutdown gracefully with a 30s timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		httpServer.Shutdown(ctx)
		metricsServer.Shutdown(ctx)
	}()

	// Cleanup
	if err := <-errc; err != nil {
		log.Errorf(ctx, err, "exiting")
	}
	cancel()
	wg.Wait()
	log.Printf(ctx, "exited")
}
