package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/clue/debug"
	"goa.design/clue/health"
	"goa.design/clue/log"
	"goa.design/clue/metrics"
	"goa.design/clue/trace"
	goahttp "goa.design/goa/v3/http"
	goahttpmiddleware "goa.design/goa/v3/http/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"goa.design/ponos/examples/weather/services/forecaster"
	"goa.design/ponos/examples/weather/services/forecaster/clients/poller"
	genforecaster "goa.design/ponos/examples/weather/services/forecaster/gen/forecaster"
	genhttp "goa.design/ponos/examples/weather/services/forecaster/gen/http/forecaster/server"
)

func main() {
	var (
		httpaddr    = flag.String("http-addr", ":8080", "HTTP listen address")
		metricsAddr = flag.String("metrics-addr", ":8081", "metrics listen address")
		polleraddr  = flag.String("poller-addr", ":8082", "Poller service HTTP address")
		redisurl    = flag.String("redis-url", "redis://default:"+os.Getenv("REDIS_PASSWORD")+"@localhost:6379/0", "Redis URL")
		agentaddr   = flag.String("agent-addr", ":4317", "Grafana agent listen address")
		debugf      = flag.Bool("debug", false, "Enable debug logs")
	)
	flag.Parse()

	// 1. Create logger
	format := log.FormatJSON
	if log.IsTerminal() {
		format = log.FormatTerminal
	}
	ctx := log.Context(context.Background(), log.WithFormat(format), log.WithFunc(trace.Log))
	ctx = log.With(ctx, log.KV{K: "svc", V: genforecaster.ServiceName})
	if *debugf {
		ctx = log.Context(ctx, log.WithDebug())
		log.Debugf(ctx, "debug logs enabled")
	}

	// 2. Setup tracing
	log.Debugf(ctx, "connecting to Grafana agent %s", *agentaddr)
	conn, err := grpc.DialContext(ctx, *agentaddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		log.Errorf(ctx, err, "failed to connect to Grafana agent")
		os.Exit(1)
	}
	log.Debugf(ctx, "connected to Grafana agent %s", *agentaddr)
	ctx, err = trace.Context(ctx, genforecaster.ServiceName, trace.WithGRPCExporter(conn))
	if err != nil {
		log.Errorf(ctx, err, "failed to initialize tracing")
		os.Exit(1)
	}

	// 3. Setup metrics
	ctx = metrics.Context(ctx, genforecaster.ServiceName)

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
	transport := trace.Client(ctx, log.Client(http.DefaultTransport))
	httpc := http.Client{Transport: transport}
	pc := poller.New(scheme, host, &httpc)

	// Ponos replicated map for forecast cache
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
	http.Handle("/healthz", check)
	http.Handle("/livez", check)
	http.Handle("/metrics", metrics.Handler(ctx).(http.HandlerFunc))
	metricsServer := &http.Server{Addr: *metricsAddr}

	// 6. Create service & endpoints
	svc := forecaster.New(ctx, pc, rdb)
	endpoints := genforecaster.NewEndpoints(svc)
	endpoints.Use(debug.LogPayloads())
	endpoints.Use(log.Endpoint)

	// 7. Create transport
	mux := goahttp.NewMuxer()
	mux.Use(metrics.HTTP(ctx))
	debug.MountDebugLogEnabler(debug.Adapt(mux))
	server := genhttp.New(endpoints, mux, goahttp.RequestDecoder, goahttp.ResponseEncoder, nil, nil)
	genhttp.Mount(mux, server)
	handler := debug.HTTP()(mux)                                               // 5. Manage debug logs dynamically
	handler = trace.HTTP(ctx)(handler)                                         // 4. Trace request (adds Trace ID to context)
	handler = goahttpmiddleware.LogContext(log.AsGoaMiddlewareLogger)(handler) // 3. Log request and response
	handler = log.HTTP(ctx)(handler)                                           // 2. Add logger to request context
	handler = goahttpmiddleware.RequestID()(handler)                           // 1. Add request ID to context
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
