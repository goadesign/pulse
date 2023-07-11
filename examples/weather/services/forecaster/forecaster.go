package forecaster

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"

	"goa.design/pulse/examples/weather/services/forecaster/clients/poller"
	genforecaster "goa.design/pulse/examples/weather/services/forecaster/gen/forecaster"
	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

type (
	// Service is the forecast service implementation.
	Service struct {
		fmap    *rmap.Map // Shared forecasts cache
		pollerc poller.Client
		wg      *sync.WaitGroup // Wait group used to wait for all goroutines to complete

		lock    sync.Mutex
		stopped bool
		subs    map[string]func() // Map of context cancel
		agg     chan *genpoller.Forecast
	}
)

// forecastCacheMap is the name of the forecasts replicated map.
const forecastCacheMap = "forecasts-cache"

// maxForecastWaitDelay is the maximum delay to wait for a forecast to be
// available in the cache. Make it a variable so it can be changed in tests.
var maxForecastWaitDelay = 10 * time.Second

// New instantiates a new forecast service.
func New(ctx context.Context, pollerc poller.Client, rdb *redis.Client) *Service {
	fmap, err := rmap.Join(ctx, forecastCacheMap, rdb, rmap.WithLogger(pulse.ClueLogger(ctx)))
	if err != nil {
		log.Errorf(ctx, err, "failed to initialize Redis map")
		os.Exit(1)
	}
	svc := &Service{
		fmap:    fmap,
		pollerc: pollerc,
		wg:      &sync.WaitGroup{},
		subs:    make(map[string]func()),
		agg:     make(chan *genpoller.Forecast),
	}
	go svc.cacheForecasts(ctx)
	return svc
}

// Forecast returns the forecast for the given location. It uses the forecasts
// replicated map as a transparent cache. If the forecast is not in the cache
// then it creates a new poll job using the poller service and waits for the
// forecast to be available in the map.
func (svc *Service) Forecast(ctx context.Context, p *genforecaster.ForecastPayload) (*genforecaster.Forecast2, error) {
	if svc.isStopped() {
		return nil, genforecaster.MakeTimeout(fmt.Errorf("service stopped"))
	}
	marshaled, ok := svc.fmap.Get(locationKey(p.City, p.State))
	if !ok {
		err := svc.pollerc.AddLocation(ctx, p.City, p.State)
		if err != nil {
			if err != poller.ErrLocationExists {
				return nil, fmt.Errorf("failed to add poll location: %s", err)
			}
		}
		if err != poller.ErrLocationExists {
			// It could be that another request for the same location is
			// already in progress in which case the map has not been updated
			// yet. In that case we don't want to create a duplicate subscription.
			if err := svc.ensureSubscribed(p.City, p.State); err != nil {
				return nil, fmt.Errorf("failed to subscribe to forecast: %s", err)
			}
		}
		marshaled, err = svc.waitForecast(ctx, p.City, p.State)
		if err == context.DeadlineExceeded {
			return nil, genforecaster.MakeTimeout(fmt.Errorf("forecast not available after %s", maxForecastWaitDelay))
		}
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve forecast: %s", err)
		}
	}
	forecast, err := unmarshalForecast([]byte(marshaled))
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal location: %s", err)
	}
	return forecast, nil
}

// Stop stops the service gracefully.
func (svc *Service) Stop() error {
	svc.lock.Lock()
	defer svc.lock.Unlock()
	for _, cancel := range svc.subs {
		cancel()
	}
	svc.fmap.Close()
	svc.wg.Wait()
	close(svc.agg)
	svc.stopped = true
	return nil
}

// isStopped returns true if the service is stopped.
func (svc *Service) isStopped() bool {
	svc.lock.Lock()
	defer svc.lock.Unlock()
	return svc.stopped
}

// ensureSubscribed ensures that the service is subscribed to the forecasts
// stream for the given location.
func (svc *Service) ensureSubscribed(city, state string) error {
	svc.lock.Lock()
	defer svc.lock.Unlock()
	key := locationKey(city, state)
	if _, ok := svc.subs[key]; !ok {
		ctx, cancel := context.WithCancel(context.Background())
		sub, err := svc.pollerc.Subscribe(ctx, city, state)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to subscribe to forecast: %s", err)
		}
		svc.wg.Add(1)
		go func(c <-chan *genpoller.Forecast) {
			defer svc.wg.Done()
			for f := range c {
				svc.agg <- f
			}
		}(sub)
		svc.subs[key] = cancel
	}
	return nil
}

// cacheForecasts listens to the forecasts stream and updates the forecasts
// map.
func (svc *Service) cacheForecasts(ctx context.Context) {
	for f := range svc.agg {
		if svc.isStopped() {
			return
		}
		key := locationKey(f.Location.City, f.Location.State)
		log.Info(ctx, log.KV{K: "msg", V: "caching forecast"}, log.KV{K: "key", V: key})
		if _, err := svc.fmap.Set(ctx, key, marshalForecast(f)); err != nil {
			log.Errorf(ctx, err, "failed to set forecast in map")
		}
	}
}

// waitForecast waits for the forecast to be available in the forecasts map.
func (svc *Service) waitForecast(ctx context.Context, city, state string) (string, error) {
	// Wait up to 10 seconds for the forecast to be available in the map.
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(maxForecastWaitDelay))
	defer cancel()
	sub := svc.fmap.Subscribe()
	defer svc.fmap.Unsubscribe(sub)
	key := locationKey(city, state)
	for {
		select {
		case <-sub:
			if marshaled, ok := svc.fmap.Get(key); ok {
				return marshaled, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// locationKey returns the key used to store the forecast in the forecasts map.
func locationKey(city, state string) string {
	city = strings.Replace(city, " ", "-", -1) // Santa Barbara == Santa-Barbara
	return fmt.Sprintf("%s/%s", strings.ToLower(state), strings.ToLower(city))
}
