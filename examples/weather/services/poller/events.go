package poller

import (
	"bytes"
	"encoding/binary"
	"io"

	genpoller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

const (
	// forecastStream is the name of the stream for the poller forecast event.
	forecastStream = "forecasts"
	// eventForecast is the name for the poller forecast event.
	eventForecast = "forecast"
)

// marshalLocation marshals a location into a byte slice using binary encoding.
func marshalLocation(l *genpoller.Location) []byte {
	stateLen := len(l.State)
	cityLen := len(l.City)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(stateLen))
	buf.Write([]byte(l.State))
	binary.Write(buf, binary.LittleEndian, uint32(cityLen))
	buf.Write([]byte(l.City))
	binary.Write(buf, binary.LittleEndian, l.Lat)
	binary.Write(buf, binary.LittleEndian, l.Long)
	return buf.Bytes()
}

// unmarshalLocation unmarshals a location from a byte slice using binary encoding.
func unmarshalLocation(data []byte) (*genpoller.Location, error) {
	buf := bytes.NewBuffer(data)
	var stateLen, cityLen uint32
	err := binary.Read(buf, binary.LittleEndian, &stateLen)
	if err != nil {
		return nil, err
	}
	stateBytes := make([]byte, stateLen)
	if _, err = io.ReadFull(buf, stateBytes); err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &cityLen)
	if err != nil {
		return nil, err
	}
	cityBytes := make([]byte, cityLen)
	if _, err = io.ReadFull(buf, cityBytes); err != nil {
		return nil, err
	}
	var lat, long float64
	err = binary.Read(buf, binary.LittleEndian, &lat)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &long)
	if err != nil {
		return nil, err
	}
	return &genpoller.Location{
		State: string(stateBytes),
		City:  string(cityBytes),
		Lat:   lat,
		Long:  long,
	}, nil
}

// marshalForecastEvent marshals a forecast into a byte slice using binary encoding.
func marshalForecastEvent(f *genpoller.Forecast) []byte {
	locBytes := marshalLocation(f.Location)
	periodCount := len(f.Periods)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(locBytes)))
	buf.Write(locBytes)
	binary.Write(buf, binary.LittleEndian, uint32(periodCount))
	for _, p := range f.Periods {
		pBytes := marshalPeriod(p)
		binary.Write(buf, binary.LittleEndian, uint32(len(pBytes)))
		buf.Write(pBytes)
	}
	return buf.Bytes()
}

// unmarshalForecastEvent unmarshals a forecast from a byte slice using binary encoding.
func unmarshalForecastEvent(data []byte) (*genpoller.Forecast, error) {
	buf := bytes.NewBuffer(data)
	var locLen, periodCount uint32
	err := binary.Read(buf, binary.LittleEndian, &locLen)
	if err != nil {
		return nil, err
	}
	locBytes := make([]byte, locLen)
	if _, err = io.ReadFull(buf, locBytes); err != nil {
		return nil, err
	}
	loc, err := unmarshalLocation(locBytes)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &periodCount)
	if err != nil {
		return nil, err
	}
	periods := make([]*genpoller.Period, periodCount)
	for i := 0; i < int(periodCount); i++ {
		var periodLen uint32
		err = binary.Read(buf, binary.LittleEndian, &periodLen)
		if err != nil {
			return nil, err
		}
		periodBytes := make([]byte, periodLen)
		if _, err = io.ReadFull(buf, periodBytes); err != nil {
			return nil, err
		}
		period, err := unmarshalPeriod(periodBytes)
		if err != nil {
			return nil, err
		}
		periods[i] = period
	}
	return &genpoller.Forecast{
		Location: loc,
		Periods:  periods,
	}, nil
}

// marshalPeriod marshals a period into a byte slice using binary encoding.
func marshalPeriod(p *genpoller.Period) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(p.Name)))
	buf.Write([]byte(p.Name))
	binary.Write(buf, binary.LittleEndian, uint32(len(p.StartTime)))
	buf.Write([]byte(p.StartTime))
	binary.Write(buf, binary.LittleEndian, uint32(len(p.EndTime)))
	buf.Write([]byte(p.EndTime))
	binary.Write(buf, binary.LittleEndian, int64(p.Temperature))
	binary.Write(buf, binary.LittleEndian, uint32(len(p.TemperatureUnit)))
	buf.Write([]byte(p.TemperatureUnit))
	binary.Write(buf, binary.LittleEndian, uint32(len(p.Summary)))
	buf.Write([]byte(p.Summary))
	return buf.Bytes()
}

func unmarshalPeriod(data []byte) (*genpoller.Period, error) {
	buf := bytes.NewBuffer(data)

	var nameLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return nil, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return nil, err
	}

	var startTimeLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &startTimeLen); err != nil {
		return nil, err
	}
	startTimeBytes := make([]byte, startTimeLen)
	if _, err := io.ReadFull(buf, startTimeBytes); err != nil {
		return nil, err
	}

	var endTimeLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &endTimeLen); err != nil {
		return nil, err
	}
	endTimeBytes := make([]byte, endTimeLen)
	if _, err := io.ReadFull(buf, endTimeBytes); err != nil {
		return nil, err
	}

	var temperature int64
	if err := binary.Read(buf, binary.LittleEndian, &temperature); err != nil {
		return nil, err
	}

	var tempUnitLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &tempUnitLen); err != nil {
		return nil, err
	}
	tempUnitBytes := make([]byte, tempUnitLen)
	if _, err := io.ReadFull(buf, tempUnitBytes); err != nil {
		return nil, err
	}

	var summaryLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &summaryLen); err != nil {
		return nil, err
	}
	summaryBytes := make([]byte, summaryLen)
	if _, err := io.ReadFull(buf, summaryBytes); err != nil {
		return nil, err
	}

	return &genpoller.Period{
		Name:            string(nameBytes),
		StartTime:       string(startTimeBytes),
		EndTime:         string(endTimeBytes),
		Temperature:     int(temperature),
		TemperatureUnit: string(tempUnitBytes),
		Summary:         string(summaryBytes),
	}, nil
}
