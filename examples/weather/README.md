# Weather Ponos Example

This example showcases the use of the various Ponos packages to build a simple
weather forecast service that minimizes API requests and provides very fast
response times. The system consists of two components:

- A poller service that leverages a Ponos worker pool and streaming to poll
        weather forecasts. The service exposes APIs to start poll jobs and subscribe
        to location specific weather forecast updates. The service also exposes
        "control" APIs to query statistics for the poll jobs, add or remove workers.

- A Forecaster service which provides HTTP APIs to query the latest forecast for
        a given location. The Forecaster service leverages the poller service to
        create new forecast poll jobs and to subscribe to updates. 

The Forecaster service employs a cache system using a Ponos replicated map to
store and retrieve the latest weather forecasts for different locations. When a
forecast request is received, the service checks the cache and returns the
cached forecast if available. In case of a cache miss, the forecaster service
makes a request to the poller service which initiates a Ponos job to fetch the
forecast from the weather API. The Forecaster service then subscribes to the
replicated map and waits for the forecast to be available in the cache.

The poll job is executed by a dedicated worker that publishes the forecasts to a
stream. The poller service subscribes to the stream and forwards forecasts to
subscribers. The Forecaster service subscribes to the poller service and updates
the cache with the latest forecasts. 

This flow ensures efficient and timely retrieval of forecasts while minimizing
API calls to the weather service.        The following diagram shows the architecture
of the weather system:

![Weather System Architecture](diagram/Weather%20System%20Services.svg)

## Running the Example

The `scripts` directory contains a couple of scripts that can be used to run the
example:

- `setup` downloads build dependencies and builds the example.
- `server` runs the services using
        [overmind](https://github.com/DarthSim/overmind). `server` also starts
        `docker-compose` with a configuration that runs Redis, the Grafana agent,
        cortex, tempo and dashboard locally.

### Making a Request

The Forecaster service exposes a simple HTTP API that can be used to query the
latest weather forecast for a given location. The following example shows how to
query the weather forecast for `ca/santa-barbara`:

```bash
curl http://localhost:8080/forecast/ca/santa-barbara
```

The response is a JSON object that contains the latest weather forecast for
`santa-barbara`:

```json
{
    "location": {
        "city": "Santa Barbara",
        "lat": 34.4221319,
        "long": -119.702667,
        "state": "CA"
    },
    "periods": [
        {
            "endTime": "2023-05-31T06:00:00-07:00",
            "name": "Tonight",
            "startTime": "2023-05-30T20:00:00-07:00",
            "summary": "Mostly Cloudy",
            "temperature": 57,
            "temperatureUnit": "F"
        },
        ...
    ]
}
```

The poller service also expose a simple HTTP API that can be used to query
status information.

```bash
curl http://localhost:8082/poller/status
```

The response is a JSON object that contains the list of workers and jobs that
are currently being executed by the service:

```json
{
    "jobs": [
        {
            "created_at": "2023-05-31T03:57:06Z",
            "key": "ca/santa-barbara",
            "payload": "AgAAAGNhDQAAAHNhbnRhLWJhcmJhcmE="
        }
    ],
    "workers": [
        {
            "created_at": "2023-05-30T20:57:05-07:00",
            "id": "01H1QZ7FNRQXPZ17KTWYDZ49SM",
            "jobs": [
                {
                    "created_at": "2023-05-31T03:57:06Z",
                    "key": "ca/santa-barbara",
                    "payload": "AgAAAGNhDQAAAHNhbnRhLWJhcmJhcmE="
                }
            ]
        }
    ]
}
```

## Code Organization

The example consists of the following directories:

- `forecaster` contains the Forecaster service.
- `poller` contains the poller service.

Both the service and the worker are implemented using the
[Goa](https://goa.design) framework.

Each service contains a `design` package that defines the service API using the Goa DSL.
The service directory itself contains the service implementation.

```
.
├── clients                         # HTTP clients for the poller service
├── cmd                                         # Service main
├── design                                # Service API definition
├── forecaster.go # Forecaster service implementation
├── gen                                         # Generated code
└── marshal.go                # Forecaster service implementation
```

The clients directory contains the downstream dependency clients used by the
service implementation. In the case of the Forecaster service there is only one
such client: the client used by the service to make requests to the poller
service.

```
clients
└── poller
    ├── client.go
    └── mocks
```

The `mocks` package contains a mock implementation of the client used by the
service. The mock is used by the service unit tests.


 ## Critical Code & Reusable Patterns

The `waitForForecast` function in the `forecaster.go` file in the Forecaster
service implements the logic to wait for a specific key to be available in a
Ponos replicated map with a specific timeout. The function makes use of a
`select` statement to wait for replicated map updates or a timeout:

```go
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
```

The `Subscribe` method of the Poller service shows how to forward Ponos stream events
to a WebSocket connection:

```go
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
```