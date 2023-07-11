package poller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

func TestMarshalLocation(t *testing.T) {
	// Create a new location.
	l := &genpoller.Location{
		Lat:   37.7749,
		Long:  -122.4194,
		City:  "San Francisco",
		State: "CA",
	}

	// Marshal the location.
	data := marshalLocation(l)

	// Verify that the length of the data is correct.
	expectedLen := 4 + len(l.State) + 4 + len(l.City) + 8 + 8
	assert.Equal(t, expectedLen, len(data))

	// Unmarshal the data.
	l2, err := unmarshalLocation(data)
	assert.NoError(t, err)

	// Verify that the unmarshaled location is equal to the original location.
	assert.Equal(t, l, l2)

	// Truncate the data to simulate a corrupted message.
	truncatedData := data[:len(data)-1]

	// Unmarshal the truncated data.
	_, err = unmarshalLocation(truncatedData)
	assert.Error(t, err)
}

func TestMarshalForecastEvent(t *testing.T) {
	// Create a new forecast event.
	f := &genpoller.Forecast{
		Location: &genpoller.Location{
			Lat:   37.7749,
			Long:  -122.4194,
			City:  "San Francisco",
			State: "CA",
		},
		Periods: []*genpoller.Period{
			{
				Name:            "Today",
				StartTime:       "2022-01-01T00:00:00Z",
				EndTime:         "2022-01-01T23:59:59Z",
				Temperature:     72,
				TemperatureUnit: "F",
				Summary:         "Sunny",
			},
			{
				Name:            "Tomorrow",
				StartTime:       "2022-01-02T00:00:00Z",
				EndTime:         "2022-01-02T23:59:59Z",
				Temperature:     68,
				TemperatureUnit: "F",
				Summary:         "Partly Cloudy",
			},
		},
	}

	// Marshal the forecast event.
	data := marshalForecastEvent(f)

	// Unmarshal the data.
	f2, err := unmarshalForecastEvent(data)
	assert.NoError(t, err)

	// Verify that the unmarshaled forecast event is equal to the original forecast event.
	assert.Equal(t, f, f2)

	// Truncate the data to simulate a corrupted message.
	truncatedData := data[:len(data)-1]

	// Unmarshal the truncated data.
	_, err = unmarshalForecastEvent(truncatedData)
	assert.Error(t, err)
}

func TestMarshalPeriod(t *testing.T) {
	// Create a new period.
	p := &genpoller.Period{
		Name:            "Today",
		StartTime:       "2022-01-01T00:00:00Z",
		EndTime:         "2022-01-01T23:59:59Z",
		Temperature:     72,
		TemperatureUnit: "F",
		Summary:         "Sunny",
	}

	// Marshal the period.
	data := marshalPeriod(p)

	// Verify that the length of the data is correct.
	expectedLen := 4 + len(p.Name) + 4 + len(p.StartTime) + 4 + len(p.EndTime) + 8 + 4 + len(p.TemperatureUnit) + 4 + len(p.Summary)
	assert.Len(t, data, expectedLen)

	// Unmarshal the data.
	p2, err := unmarshalPeriod(data)
	assert.NoError(t, err)

	// Verify that the unmarshaled period is equal to the original period.
	assert.Equal(t, p, p2)
}
