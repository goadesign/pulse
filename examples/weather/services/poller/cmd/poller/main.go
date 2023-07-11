package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
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

	"goa.design/pulse/examples/weather/services/poller"
	"goa.design/pulse/examples/weather/services/poller/clients/nominatim"
	"goa.design/pulse/examples/weather/services/poller/clients/weathergov"
	genhttp "goa.design/pulse/examples/weather/services/poller/gen/http/poller/server"
	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

func main() {
	var (
		httpaddr    = flag.String("http-addr", ":8082", "HTTP listen address")
		metricsAddr = flag.String("metrics-addr", ":8083", "metrics listen address")
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
	ctx = log.With(ctx, log.KV{K: "svc", V: genpoller.ServiceName})
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
	ctx, err = trace.Context(ctx, genpoller.ServiceName, trace.WithGRPCExporter(conn))
	if err != nil {
		log.Errorf(ctx, err, "failed to initialize tracing")
		os.Exit(1)
	}

	// 3. Setup metrics
	ctx = metrics.Context(ctx, genpoller.ServiceName)

	// 4. Create clients
	transport := trace.Client(ctx, log.Client(http.DefaultTransport))
	httpc := http.Client{Transport: transport}
	nominatimc := nominatim.New(&httpc)
	weatherc := weathergov.New(&httpc)
	opt, err := redis.ParseURL(*redisurl)
	if err != nil {
		log.Errorf(ctx, err, "failed to parse Redis URL")
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)

	// 5. Mount health check & metrics on separate HTTP server (different listen port)
	check := health.Handler(health.NewChecker())
	http.Handle("/healthz", check)
	http.Handle("/livez", check)
	http.Handle("/metrics", metrics.Handler(ctx).(http.HandlerFunc))
	metricsServer := &http.Server{Addr: *metricsAddr}

	// 6. Create service & endpoints
	svc, err := poller.New(ctx, nominatimc, weatherc, rdb)
	if err != nil {
		log.Errorf(ctx, err, "failed to initialize service")
		os.Exit(1)
	}
	endpoints := genpoller.NewEndpoints(svc)
	endpoints.Use(debug.LogPayloads())
	endpoints.Use(log.Endpoint)

	// 7. Create transport
	mux := goahttp.NewMuxer()
	mux.Use(metrics.HTTP(ctx))
	debug.MountDebugLogEnabler(debug.Adapt(mux))
	server := genhttp.New(endpoints, mux, goahttp.RequestDecoder, goahttp.ResponseEncoder, nil, nil, &websocket.Upgrader{}, nil)
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

		// Shutdown gracefully with a 30s timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := svc.Stop(ctx); err != nil {
			log.Errorf(ctx, err, "failed to close service")
		}

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
