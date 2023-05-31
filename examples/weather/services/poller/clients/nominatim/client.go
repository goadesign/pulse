package nominatim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"goa.design/clue/log"
)

type (
	// Client is a client for the Nominatim API.
	Client interface {
		// Search searches for a location with the given query.
		Search(ctx context.Context, query string) ([]*Location, error)
		// Name provides a client name used to report health check issues.
		Name() string
		// Ping checks the client is healthy.
		Ping(ctx context.Context) error
	}

	// Location represents a geographical location.
	Location struct {
		// Lat is the latitude of the location.
		Lat string `json:"lat"`
		// Long is the longitude of the location.
		Long string `json:"lon"`
		// DisplayName is the display name of the location.
		DisplayName string `json:"display_name"`
	}

	// nominatimClient is a client for the Nominatim API.
	nominatimClient struct {
		baseURL string
		httpc   *http.Client
	}
)

// New creates a new Nominatim API client with the given base URL and HTTP client.
func New(httpClient *http.Client) Client {
	return &nominatimClient{
		baseURL: "https://nominatim.openstreetmap.org",
		httpc:   httpClient,
	}
}

// Search searches for a location with the given query.
func (c *nominatimClient) Search(ctx context.Context, query string) ([]*Location, error) {
	u, err := url.Parse(fmt.Sprintf("%s/search", c.baseURL))
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("q", query)
	q.Set("format", "jsonv2")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	log.Debugf(ctx, "nominatim response: %s", string(body))
	resp.Body = io.NopCloser(bytes.NewBuffer(body))
	var locations []*Location
	err = json.NewDecoder(resp.Body).Decode(&locations)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return locations, nil
}

// Name provides a client name used to report health check issues.
func (c *nominatimClient) Name() string {
	return "nominatim"
}

// Ping checks the client is healthy.
func (c *nominatimClient) Ping(ctx context.Context) error {
	u, err := url.Parse(fmt.Sprintf("%s/ping", c.baseURL))
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
