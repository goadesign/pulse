package design

import (
	. "goa.design/goa/v3/dsl"
	. "goa.design/pulse/examples/weather/design"
)

var _ = API("Forecast Poller Service API", func() {
	Title("The Poller Service API")
	Description("US weather forecast poller service API")
	HTTP(func() {
		Path("/poller")
	})
})

var _ = Service("Poller", func() {
	Description("Service that polls weather forecasts and notifies subscribers.")

	Method("add_location", func() {
		Description("Adds a location to the polling list.")
		Payload(CityAndState)
		Error("location_exists", ErrorResult, "Location already exists")
		HTTP(func() {
			POST("/location")
			Param("city")
			Param("state")
			Response(StatusAccepted)
			Response("location_exists", StatusConflict)
		})
	})

	Method("subscribe", func() {
		Description("Subscribes to weather forecast notifications for a location.")
		Payload(CityAndState)
		StreamingResult(Forecast)
		HTTP(func() {
			GET("/subscribe")
			Param("city")
			Param("state")
		})
	})

	// The following endpoints are for controlling the worker pool. They are
	// not meant to be used directly by clients (e.g. the forecaster
	// service) but are exposed for enabling auto-scaling solutions and
	// troubleshooting.

	Method("add_worker", func() {
		Description("Adds a worker to the poller worker pool.")
		Result(Worker)
		HTTP(func() {
			POST("/workers")
		})
	})

	Method("remove_worker", func() {
		Description("Removes a worker from the poller worker pool.")
		Error("too_few", ErrorResult, "Only one worker left")
		HTTP(func() {
			DELETE("/workers")
			Response("too_few", StatusExpectationFailed)
		})
	})

	Method("status", func() {
		Description("Provides poller status and statistics.")
		Result(PollerStatus)
		HTTP(func() {
			GET("/status")
		})
	})
})

var CityAndState = Type("CityAndState", func() {
	Description("Location city and state.")
	Attribute("state", String, "State", func() {
		Example("CA")
		MinLength(2)
		MaxLength(2)
	})
	Attribute("city", String, "City", func() {
		Example("Santa Barbara")
	})
	Required("city", "state")
})

var PollerStatus = Type("PollerStatus", func() {
	Description("Poller status")
	Attribute("workers", ArrayOf(Worker), "Poller workers")
	Attribute("jobs", ArrayOf(Job), "Location poll jobs")
	Required("workers", "jobs")
})

var Worker = Type("Worker", func() {
	Description("Poller worker")
	Attribute("id", String, "Worker ID", func() {
		Example("1")
	})
	Attribute("jobs", ArrayOf(Job), "Worker poll jobs")
	Attribute("created_at", String, "Creation timestamp", func() {
		Example("2020-09-01T12:00:00Z")
	})
	Required("id", "jobs", "created_at")
})

var Job = Type("Job", func() {
	Description("Location forecast poll job")
	Attribute("key", String, "Job key", func() {
		Example("1")
	})
	Attribute("payload", Bytes, "Job payload", func() {
		Example([]byte(`{"state":"CA","city":"Santa Barbara"}`))
	})
	Attribute("created_at", String, "Creation timestamp", func() {
		Example("2020-09-01T12:00:00Z")
	})
	Required("key", "payload", "created_at")
})
