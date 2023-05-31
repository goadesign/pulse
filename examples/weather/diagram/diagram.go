package model

import . "goa.design/model/dsl"

var _ = Design("Weather System Architecture", "The Weather example system architecture", func() {
	var WeatherGov = SoftwareSystem("weather.gov", "Provides weather forecasts for US locations.", func() {
		External()
		URL("https://weather.go")
		Tag("External")
	})

	var _ = Person("User", "A client of the weather system.", func() {
		Uses("Weather Software System/Forecaster Service", "Makes requests to", "HTTP", Synchronous)
	})

	var System = SoftwareSystem("Weather Software System", "Provides location based weather forecasts.", func() {
		var ForecastStream = Container("Forecast Stream", "Ponos stream used by the Poller service worker to stream forecast updates.", "Go and Ponos", func() {
			Tag("Ponos")
		})
		var WorkerPool = Container("Worker Pool", "Polls forecasts from weather.gov (code runs in Poller service process).", "Go and Ponos", func() {
			Uses(WeatherGov, "Makes requests to", "HTTP", Synchronous)
			Tag("Pool")
		})
		var Poller = Container("Poller Service", "Leverages Ponos to poll weather.gov for weather forecasts.", "Go and Goa", func() {
			Uses(ForecastStream, "Publishes forecast updates to", "Ponos", Synchronous)
			Uses(ForecastStream, "Subscribes to forecast updates from", "Ponos", Asynchronous, func() {
				Tag("Events")
			})
			Uses(WorkerPool, "Uses", "Ponos", Synchronous)
			Tag("Service")
		})
		Container("Forecaster Service", "Leverages Ponos to provide fast and efficient weather forecasts for US based locations.", "Go and Goa", func() {
			Uses(Poller, "Creates new location forecast poll jobs using", "Goa", Synchronous)
			Uses(Poller, "Subscribes to forecast updates from", "Ponos", Asynchronous)
			Tag("Service")
		})
	})

	Views(func() {
		ContainerView(System, "Weather System Services", "Weather software system architecture diagram", func() {
			AddAll()
			AutoLayout(RankLeftRight)
		})
		Styles(func() {
			ElementStyle("Person", func() {
				Background("#e6e6ea")
				Stroke("#f75c03")
				Shape(ShapePerson)
			})
			ElementStyle("Container", func() {
				Background("#e6e6ea")
				Stroke("#2ab7ca")
			})
			ElementStyle("External", func() {
				Background("#eae6e6")
				Stroke("#cab72a")
			})
			ElementStyle("Software System", func() {
				Shape(ShapeRoundedBox)
				Background("#e6e6ea")
				Stroke("#f75c03")
			})
			ElementStyle("Ponos", func() {
				Shape(ShapePipe)
				Background("#e6e6ea")
				Stroke("#f75c03")
			})
			ElementStyle("Pool", func() {
				Shape(ShapeFolder)
				Background("#e6e6ea")
				Stroke("#f75c03")
			})
			RelationshipStyle("Asynchronous", func() {
				Dashed()
				Stroke("#f75c03")
			})
			RelationshipStyle("default", func() {
				Solid()
				Stroke("#f75c03")
			})
		})
	})
})
