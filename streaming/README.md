# Streaming

Ponos leverages Redis streams to provide scalable and reliable event streams
that can be used to implement distributed architectures. Ponos provides a simple
API to create and consume streams, for example:

https://github.com/goadesign/ponos/tree/main/examples/streaming/single-reader/main.go#L1-L38

The code above creates a stream named "my-stream" and adds a new event to it.
The event is then consumed by a reader. The reader is closed after the event
is consumed.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    A[Event Producer]
    subgraph SA[Stream A]
        TA[Topic]
    end
    subgraph SB[Stream B]
        TB[Topic]
    end
    B[Event Consumer]
    A-->|Add|TA
    A-->|Add|TB
    TA-.->|Event|B
    TB-.->|Event|B

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef ponos fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class A,B userCode;
    class SA,SB,TA,TB ponos;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

Multiple readers can be created for the same stream across many nodes. Readers
are independent and each instance receives a copy of the same events. Readers
can specify a start position for the stream cursor. The default start position
is the last event in the stream.

https://github.com/goadesign/ponos/blob/main/examples/streaming/multi-reader/main.go#L18-L36

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    A[Event Producer]
    subgraph SA[Stream A]
        TA[Topic]
    end
    subgraph SB[Stream B]
        TB[Topic]
    end
    B[Event Consumer]
    A-->|Add|TA
    A-->|Add|TB
    TA-.->|Event|B
    TB-.->|Event|B

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef ponos fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class A,B userCode;
    class SA,SB,TA,TB ponos;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

## Event Sinks

Event sinks enable concurrent processing of multiple events for better
performance. They also enable redundancy in case of node failure or network
partitions.

Event sinks make it possible for multiple nodes to share the same stream cursor.
If a stream contains 3 events and 3 nodes are consuming the stream using the
same sink (i.e. a sink with the same name), then each node will receive a unique
event from the sequence. Nodes using a different sink (or a reader) will receive
copies of the same events.  

Sink events must be acknowledged by the client. Ponos automatically requeues
events added to a sink that have been read by a node but not acknowledged.

Creating a sink is as simple as:

https://github.com/goadesign/ponos/blob/main/examples/streaming/single-sink/main.go#L1-L43

Note a couple of differences with the reader example above:

- The sink is created using `stream.NewSink` instead of `stream.NewReader`. Each
  sink has a unique name, multiple nodes using the same name will share the same
  stream cursor.
- The event is acknowledged using `sink.Ack`. This provides an at-least-once
  delivery guarantee where unacknowledged events are automatically re-queued.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    Producer-->|Add Event|Stream
    subgraph Stream ["Stream #quot;my-stream#quot;"]
        direction TB
        A[fa:fa-bolt Event 1]-.-B[fa:fa-bolt Event n]
    end
    subgraph Sink1 ["Sink #quot;my-sink#quot;"]
        direction LR
        C[fa:fa-microchip Process]-.-D[fa:fa-microchip Process]
    end
    Stream -->|fa:fa-bolt Event 1..n|Sink1

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef ponos fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class A,B userCode;
    class Map ponos;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

As with readers, multiple sinks can be created for the same stream. Copies of
the same event are distributed among all sinks.

```mermaid
flowchart TD
    subgraph Producers
        direction LR
        H[fa:fa-microchip Process]-.-G[fa:fa-microchip Process]
    end
    Producers-->|fa:fa-bolt Add event|Stream
    subgraph Stream ["Stream #quot;my-stream#quot;"]
        direction TB
        A[fa:fa-bolt Event 1]-.-B[fa:fa-bolt Event n]
    end
    subgraph Sink1 ["Sink #quot;my-sink#quot;"]
        direction LR
        C[fa:fa-microchip Process]-.-D[fa:fa-microchip Process]
    end
    subgraph Sink2 ["Sink #quot;my-other-sink#quot;"]
        direction LR
        E[fa:fa-microchip Process]-.-F[fa:fa-microchip Process]
    end
    Stream -->|fa:fa-bolt Event 1..n|Sink1
    Stream -->|fa:fa-bolt Event 1..n|Sink2
```

## Reading from multiple streams

Readers and sinks can also read concurrently from multiple streams.  For
example:

```go
// Create stream "my-stream"
stream, err := streaming.NewStream(context.Background(), "my-stream", rdb)
if err != nil {
    panic(err)
}

// Create sink "my-sink" for stream "my-stream"
sink := stream.NewSink("my-sink")
defer sink.Close()

// Create stream "my-other-stream"
otherStream := streaming.NewStream(context.Background(), "my-other-stream", rdb)

// Add stream "my-other-stream" to sink "my-sink"
sink.AddStream(otherStream)

// Consume events from both streams
for event := range sink.C {
    fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.Event, event.Payload)
    event.Ack()
}
```

```mermaid
flowchart TD
    subgraph Producers
        direction LR
        H[fa:fa-microchip Process]-.-G[fa:fa-microchip Process]
    end
    subgraph Producers2 [Other producers]
        direction LR
        H2[fa:fa-microchip Process]-.-G2[fa:fa-microchip Process]
    end
    Producers-->|fa:fa-bolt Add event|Stream
    Producers2-->|fa:fa-bolt Add event|Stream2
    subgraph Stream ["Stream #quot;my-stream#quot;"]
        direction TB
        A[fa:fa-bolt Event 1]-.-B[fa:fa-bolt Event n]
    end
    subgraph Stream2 ["Stream #quot;my-other-stream#quot;"]
        direction TB
        C[fa:fa-bolt Event 1]-.-D[fa:fa-bolt Event n]
    end
    subgraph Sink ["Sink #quot;my-sink#quot;"]
        direction LR
        E[fa:fa-microchip Process]-.-F[fa:fa-microchip Process]
    end
    Stream -->|fa:fa-bolt Event 1..n|Sink
    Stream2 -->|fa:fa-bolt Event 1..n|Sink
```

`AddStream` can be called at any time to add new streams to a reader or a sink.
Streams can also be removed using `RemoveStream`.

```go
// Remove stream "my-other-stream" from sink "my-sink"
sink.RemoveStream(otherStream)
```

## Pub/Sub

Streams supports a flexible pub/sub mechanism where events can be attached to
topics and readers or sinks can define simple or custom matching logic.

```go
// Create topics "my-topic" and "my-other-topic"
topic := stream.NewTopic("my-topic")
otherTopic := stream.NewTopic("my-other-topic")

// Add a new event to topic "my-topic"
if err := topic.Add(ctx, "event", "payload"); err != nil {
    panic(err)
}

// Add a new event to topic "my-other-topic"
if err := otherTopic.Add(ctx, "event", "payload"); err != nil {
    panic(err)
}

// Consume events for topic "my-topic"
sink, err := stream.NewSink(ctx, "my-sink", ponos.WithSinkTopic("my-topic"))
defer sink.Close()
for event := range sink.C {
    fmt.Printf("event: %s, payload: %s\n", event.EventName, event.Payload)
    event.Ack()
}
```

```mermaid
flowchart TD
    subgraph Producers
        direction LR
        H[fa:fa-microchip Process]-.-G[fa:fa-microchip Process]
    end
    Producers-->|fa:fa-bolt Add event|Topic
    Producers-->|fa:fa-bolt Add event|Topic2
    subgraph Stream ["Stream #quot;my-stream#quot;"]
        direction TB
        subgraph Topic ["Topic #quot;my-topic#quot;"]
            direction TB
            A[fa:fa-bolt Event 1]-.-B[fa:fa-bolt Event n]
        end
        subgraph Topic2 ["Topic #quot;my-other-topic#quot;"]
            direction TB
            A2[fa:fa-bolt Event 1]-.-B2[fa:fa-bolt Event n]
        end
    end
    subgraph Sink ["Sink #quot;my-sink#quot;"]
        direction LR
        C[fa:fa-microchip Process]-.-D[fa:fa-microchip Process]
    end
    Topic -->|fa:fa-bolt Event 1..n|Sink
```

Topics can be matched using their name as in the example above or using complex
patterns. For example:

```go
sink, err := stream.NewSink(ctx, "my-sink", ponos.WithSinkTopicPattern("my-topic.*"))
```

Custom matching logic can also be provided:

```go
sink, err := stream.NewSink(ctx, "my-sink", ponos.WithSinkEventMatcher(
    func(event *ponos.Event) bool {
        return event.Topic == "my-topic" && event.EventName == "event"
    }))
```

> Note: Event filtering is done locally in the sink or reader and does not
> affect the underlying stream. This means that events are still stored in the
> stream and can be consumed by other sinks.