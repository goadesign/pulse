package design

import (
	. "goa.design/goa/v3/dsl"
)

var Forecast = Type("Forecast", func() {
	Description("Weather forecast")
	Attribute("location", Location, "Forecast location")
	Attribute("periods", ArrayOf(Period), "Weather forecast periods")
	Required("location", "periods")
})

var Location = Type("Location", func() {
	Description("Geographical location")
	Attribute("lat", Float64, "Latitude", func() {
		Example(34.4237458)
	})
	Attribute("long", Float64, "Longitude", func() {
		Example(-119.7049146)
	})
	Attribute("city", String, "City", func() {
		Example("Santa Barbara")
	})
	Attribute("state", String, "State", func() {
		Example("CA")
	})
	Required("lat", "long", "city", "state")
})

var Period = Type("Period", func() {
	Description("Weather forecast period")
	Attribute("name", String, "Period name", func() {
		Example("Morning")
	})
	Attribute("startTime", String, "Start time", func() {
		Format(FormatDateTime)
		Example("2020-01-01T00:00:00Z")
	})
	Attribute("endTime", String, "End time", func() {
		Format(FormatDateTime)
		Example("2020-01-01T00:00:00Z")
	})
	Attribute("temperature", Int, "Temperature", func() {
		Example(70)
	})
	Attribute("temperatureUnit", String, "Temperature unit", func() {
		Example("F")
	})
	Attribute("summary", String, "Summary", func() {
		Example("Clear")
	})
	Required("name", "startTime", "endTime", "temperature", "temperatureUnit", "summary")
})
