/*
Package poller implements a simple poller for the weather service.
It consists of a Pulse worker that periodically calls the weather service
to retrieve the weather forecast for given US cities.
*/
package poller

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"goa.design/clue/log"
	"goa.design/pulse/pool"
	"goa.design/pulse/streaming"
	"goa.design/pulse/streaming/options"

	"goa.design/pulse/examples/weather/services/poller/clients/nominatim"
	"goa.design/pulse/examples/weather/services/poller/clients/weathergov"
	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

type (
	// Worker is the type that implements the Pulse worker interface.
	Worker struct {
		// nominatimc is the Nominatim client.
		nominatimc nominatim.Client
		// weatherc is weather service client.
		weatherc weathergov.Client
		// updateStream is the Pulse update stream.
		updateStream *streaming.Stream
		// Lock protects the locations map.
		lock sync.Mutex
		// stops is the list of stops to poll.
		stops map[string]chan struct{}
		// logContext is the context used to log and trace.
		logContext context.Context
	}

	// geocodingResponse is the type that represents the response from the
	// Nominatim API.
	geocodingResponse struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	}
)

// errLocationExists is the error returned by AddLocation when the location is
// already being polled.
var errLocationExists = fmt.Errorf("location already exists")

// New returns a new poller.
func NewWorker(ctx context.Context, updateStream *streaming.Stream, nominatimc nominatim.Client, weatherc weathergov.Client) *Worker {
	return &Worker{
		nominatimc:   nominatimc,
		weatherc:     weatherc,
		updateStream: updateStream,
		stops:        make(map[string]chan struct{}),
		logContext:   ctx,
	}
}

// Start starts polling the weather service for the city and state encoded in
// the Pulse job payload and associates the poller with the given key.
func (worker *Worker) Start(job *pool.Job) error {
	ctx := worker.logContext
	city, state, err := unmarshalCityAndState(job.Payload)
	if err != nil {
		return err
	}
	key := locationKey(city, state)
	worker.lock.Lock()
	defer worker.lock.Unlock()
	if _, ok := worker.stops[key]; ok {
		log.Infof(ctx, "already polling %s, %s", city, state)
		return errLocationExists
	}
	loc, err := worker.nominatimc.Search(ctx, fmt.Sprintf("%s, %s", city, state))
	if err != nil {
		return fmt.Errorf("failed to get lat/long for %s, %s: %s", city, state, err)
	}
	if len(loc) == 0 {
		return fmt.Errorf("no location found for %s, %s", city, state)
	}
	stop := make(chan struct{})
	lat, err := strconv.ParseFloat(loc[0].Lat, 64)
	if err != nil {
		return fmt.Errorf("invalid latitude %s: %s", loc[0].Lat, err)
	}
	long, err := strconv.ParseFloat(loc[0].Long, 64)
	if err != nil {
		return fmt.Errorf("invalid longitude %s: %s", loc[0].Long, err)
	}
	go worker.poll(ctx, key, lat, long, stop)
	worker.stops[key] = stop
	log.Info(ctx, log.KV{K: "msg", V: "polling"}, log.KV{K: "city", V: city}, log.KV{K: "state", V: state}, log.KV{K: "key", V: key})
	return nil
}

// Stop stops the poller associated with the given key.
func (worker *Worker) Stop(key string) error {
	ctx := worker.logContext
	worker.lock.Lock()
	defer worker.lock.Unlock()
	c, ok := worker.stops[key]
	if !ok {
		log.Info(ctx, log.KV{K: "msg", V: "not polling"}, log.KV{K: "key", V: key})
		return nil
	}
	close(c)
	delete(worker.stops, key)
	log.Info(ctx, log.KV{K: "msg", V: "stopped polling"}, log.KV{K: "key", V: key})
	return nil
}

// poll polls the weather service for the given location.
func (worker *Worker) poll(ctx context.Context, key string, lat, long float64, stop chan struct{}) {
	worker.pollOnce(ctx, key, lat, long)
	for {
		select {
		case <-stop:
			return
		case <-time.After(time.Hour):
			worker.pollOnce(ctx, key, lat, long)
		}
	}
}

// pollOnce polls the weather service for the given location once.
func (worker *Worker) pollOnce(ctx context.Context, key string, lat, long float64) {
	forecast, err := worker.weatherc.GetForecast(ctx, lat, long)
	if err != nil {
		log.Errorf(ctx, err, "failed to get forecast")
		return
	}
	log.Infof(ctx, "forecast for %s, %s: %d", forecast.Location.City, forecast.Location.State, forecast.Periods[0].Temperature)
	if _, err := worker.updateStream.Add(
		ctx,
		eventForecast,
		marshalForecastEvent(toForecast(forecast)),
		options.WithTopic(key),
	); err != nil {
		log.Errorf(ctx, err, "failed to add forecast event")
	}
}

// toForecast converts a weathergov.Forecast to a genpoller.Forecast.
func toForecast(f *weathergov.Forecast) *genpoller.Forecast {
	periods := make([]*genpoller.Period, len(f.Periods))
	for i, p := range f.Periods {
		periods[i] = (*genpoller.Period)(p)
	}
	return &genpoller.Forecast{
		Location: (*genpoller.Location)(f.Location),
		Periods:  periods,
	}
}
