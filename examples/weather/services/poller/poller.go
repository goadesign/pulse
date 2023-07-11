package poller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/pulse/pool"
	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming"
	"goa.design/pulse/streaming/options"

	"goa.design/pulse/examples/weather/services/poller/clients/nominatim"
	"goa.design/pulse/examples/weather/services/poller/clients/weathergov"
	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

type (
	// Service is the poller service implementation.
	Service struct {
		nominatimc nominatim.Client
		weatherc   weathergov.Client
		stream     *streaming.Stream // Forecasts stream
		node       *pool.Node
	}
)

// poolName is the name of the pool of workers used by the poller service.
const poolName = "forecast-pollers"

// New instantiates a new poller service.
func New(ctx context.Context, nominatimc nominatim.Client, weatherc weathergov.Client, rdb *redis.Client) (*Service, error) {
	stream, err := streaming.NewStream(forecastStream, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %s", err)
	}
	node, err := pool.AddNode(ctx, poolName, rdb, pool.WithLogger(pulse.ClueLogger(ctx)))
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %s", err)
	}
	svc := &Service{
		nominatimc: nominatimc,
		weatherc:   weatherc,
		stream:     stream,
		node:       node,
	}
	if _, err := svc.AddWorker(ctx); err != nil {
		return nil, fmt.Errorf("failed to add worker: %s", err)
	}
	return svc, nil
}

// AddJob adds a new job to the poller worker pool.
func (svc *Service) AddLocation(ctx context.Context, p *genpoller.CityAndState) error {
	payload := marshalCityAndState(p.City, p.State)
	if err := svc.node.DispatchJob(ctx, locationKey(p.City, p.State), payload); err != nil {
		if err == errLocationExists {
			return genpoller.MakeLocationExists(err)
		}
		return fmt.Errorf("failed to dispatch job: %s", err)
	}
	return nil
}

// Subscribe subscribes to forecasts for the given location.
func (svc *Service) Subscribe(ctx context.Context, location *genpoller.CityAndState, sub genpoller.SubscribeServerStream) error {
	// Create stream reader to retrieve events from the stream.
	r, err := svc.stream.NewReader(ctx,
		options.WithReaderTopic(locationKey(location.City, location.State)),
		options.WithReaderStartAt(time.Now().Add(-time.Hour)),
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream: %s", err)
	}
	// Don't forget to close the reader when done.
	defer r.Close()
	// Subscribe to the stream.
	ch := r.Subscribe()
	for {
		select {
		case <-ctx.Done():
			// Client closed the connection
			return nil
		case f, ok := <-ch:
			if !ok {
				// Service is shutting down.
				return nil
			}
			// Got an event, unmarshal it and send it to the client.
			ev, err := unmarshalForecastEvent(f.Payload)
			if err != nil {
				log.Errorf(ctx, err, "failed to unmarshal forecast event")
				continue
			}
			if err := sub.Send(ev); err != nil {
				return fmt.Errorf("failed to send forecast: %s", err)
			}
		}
	}
}

// AddWorker adds a new worker to the poller worker pool.
func (svc *Service) AddWorker(ctx context.Context) (*genpoller.Worker, error) {
	w := NewWorker(ctx, svc.stream, svc.nominatimc, svc.weatherc)
	pw, err := svc.node.AddWorker(ctx, w)
	if err != nil {
		return nil, fmt.Errorf("failed to add worker: %s", err)
	}
	return &genpoller.Worker{
		ID:        pw.ID,
		CreatedAt: pw.CreatedAt.Format(time.RFC3339),
	}, nil
}

// RemoveWorker removes a worker from the poller worker pool.
func (svc *Service) RemoveWorker(ctx context.Context) error {
	workers := svc.node.Workers()
	if len(workers) == 1 {
		return genpoller.MakeTooFew(fmt.Errorf("cannot remove worker, only one left"))
	}
	if err := svc.node.RemoveWorker(ctx, workers[0]); err != nil {
		return fmt.Errorf("failed to remove worker: %s", err)
	}
	return nil
}

// Status returns the status and statistics of the poller service.
func (svc *Service) Status(ctx context.Context) (*genpoller.PollerStatus, error) {
	workers := svc.node.Workers()
	var jobs []*genpoller.Job
	pws := make([]*genpoller.Worker, 0, len(workers))
	for _, w := range workers {
		wjs := w.Jobs()
		pwjs := make([]*genpoller.Job, 0, len(wjs))
		for _, j := range wjs {
			pwjs = append(pwjs, &genpoller.Job{
				Key:       j.Key,
				Payload:   j.Payload,
				CreatedAt: j.CreatedAt.Format(time.RFC3339),
			})
		}
		pw := &genpoller.Worker{
			ID:        w.ID,
			Jobs:      pwjs,
			CreatedAt: w.CreatedAt.Format(time.RFC3339),
		}
		pws = append(pws, pw)
		jobs = append(jobs, pwjs...)
	}
	return &genpoller.PollerStatus{
		Workers: pws,
		Jobs:    jobs,
	}, nil
}

// Stop stops the poller service gracefully.
func (svc *Service) Stop(ctx context.Context) error {
	return svc.node.Close(ctx)
}

// locationKey returns the key used to store the forecast in the forecasts map.
func locationKey(city, state string) string {
	city = strings.Replace(city, " ", "-", -1) // Santa Barbara == Santa-Barbara
	return fmt.Sprintf("%s/%s", strings.ToLower(state), strings.ToLower(city))
}
