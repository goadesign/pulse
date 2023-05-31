package design

import (
	. "goa.design/goa/v3/dsl"
	. "goa.design/ponos/examples/weather/design"
)

var _ = API("Weather Forecaster Service API", func() {
	Title("The Forecaster Service API")
	Description("A fast US weather forecast service API")
	HTTP(func() {
		Path("/forecast")
	})
})

var _ = Service("Forecaster", func() {
	Description("Service that provides weather forecasts")

	Method("forecast", func() {
		Description("Retrieve weather forecast for a given US city")
		Payload(func() {
			Attribute("state", String, "State", func() {
				Example("CA")
				MinLength(2)
				MaxLength(2)
			})
			Attribute("city", String, "City", func() {
				Example("Santa Barbara")
			})
			Required("state", "city")
		})
		Result(Forecast)
		Error("timeout", ErrorResult, "Timeout retrieving forecast")
		HTTP(func() {
			GET("/{state}/{city}")
			Response(StatusOK)
			Response("timeout", StatusGatewayTimeout)
		})
	})
})
