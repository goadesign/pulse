package poller

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
	"goa.design/clue/debug"
	"goa.design/clue/log"
	goahttp "goa.design/goa/v3/http"
	goa "goa.design/goa/v3/pkg"

	genpollerc "goa.design/pulse/examples/weather/services/poller/gen/http/poller/client"
	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

type (
	// Client is the poller client interface.
	Client interface {
		// AddLocation creates a new forecast poll job for the given location.
		AddLocation(ctx context.Context, city, state string) error

		// Subscribe subscribes to the poller service events.
		// Cancel the context to unsubscribe and return.
		Subscribe(ctx context.Context, city, state string) (<-chan *genpoller.Forecast, error)
	}

	// client implements the Client interface.
	client struct {
		addLocation goa.Endpoint
		subscribe   goa.Endpoint
	}
)

// ErrLocationExists is the error returned by AddLocation when the location is
// already being polled.
var ErrLocationExists = fmt.Errorf("location already exists")

// New returns a new forecast poller client.
func New(scheme, host string, httpc *http.Client) Client {
	c := genpollerc.NewClient(
		scheme,
		host,
		httpc,
		goahttp.RequestEncoder,
		goahttp.ResponseDecoder,
		false,
		websocket.DefaultDialer,
		nil,
	)
	return &client{
		addLocation: debug.LogPayloads(debug.WithClient())(c.AddLocation()),
		subscribe:   debug.LogPayloads(debug.WithClient())(c.Subscribe()),
	}
}

// AddLocation creates a new forecast poll job for the given location.
func (c *client) AddLocation(ctx context.Context, city, state string) error {
	_, err := c.addLocation(ctx, &genpoller.CityAndState{City: city, State: state})
	if err != nil {
		var gerr *goa.ServiceError
		if errors.As(err, &gerr) && gerr.Name == "location_exists" {
			return ErrLocationExists
		}
		return fmt.Errorf("failed to add location %s, %s: %w", city, state, err)
	}
	return nil
}

// Subscribe subscribes to the forecasts for the given location.
// Cancel the context to unsubscribe and free resources.
func (c *client) Subscribe(ctx context.Context, city, state string) (<-chan *genpoller.Forecast, error) {
	res, err := c.subscribe(ctx, &genpoller.CityAndState{City: city, State: state})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to forecasts for %s, %s: %w", city, state, err)
	}
	sub := res.(genpoller.SubscribeClientStream)
	ch := make(chan *genpoller.Forecast)
	go func() {
		defer close(ch)
		ctx = log.With(ctx, log.KV{K: "city", V: city}, log.KV{K: "state", V: state})
		log.Infof(ctx, "subscribed")
		for {
			select {
			case <-ctx.Done():
				log.Infof(ctx, "unsubscribed")
				return
			default:
				f, err := sub.Recv()
				if err != nil {
					log.Errorf(ctx, err, "failed to receive forecast event")
				}
				log.Infof(ctx, "event")
				ch <- f
			}
		}
	}()
	return ch, nil
}
