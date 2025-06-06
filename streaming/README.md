# Streaming

Pulse leverages Redis streams to provide scalable and reliable event streams
that can be used to implement distributed architectures. Pulse provides a simple
API to create and consume streams, for example:

[![Single Reader](../snippets/single-reader.png)](../examples/streaming/single-reader/main.go#L21-L51)

The code above creates a stream and adds a new event to it.  The event is then
consumed by a reader. The reader is closed after the event is consumed.

```mermaid
%%{init: {'themeVariables': { 'background': '#282828', 'mainBkg': '#282828', 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    main-->|Add|Stream
    Stream-.->|Event|Reader
    Reader-.->|Event|main

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class main userCode;
    class Stream,Reader pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

Multiple readers can be created for the same stream across many nodes. Readers
are independent and each instance receives a copy of the same events. Readers
can specify a start position for the stream cursor. The default start position
is the last event in the stream.

[![Multi Reader](../snippets/multi-reader.png)](../examples/streaming/multi-readers/main.go#L45-L72)

```mermaid
%%{init: {'themeVariables': { 'background': '#282828', 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    main-->|Add 1, 2|Stream
    Reader-.->|Events 1, 2|main
    Reader2-.->|Event 2|main
    Stream-.->|Events 1, 2|Reader
    Stream-.->|Event 2|Reader2[Other Reader]

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class main userCode;
    class Stream,Reader,Reader2 pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 4 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

## Event Sinks

Event sinks enable concurrent processing of a sequence of events for better
performance. They also enable redundancy in case of node failure or network
partitions.

Event sinks make it possible for multiple nodes to share the same stream cursor.
If a stream contains 3 events and 3 nodes are consuming the stream using the
same sink (i.e. a sink with the same name), then each node will receive a unique
event from the sequence. Nodes using a different sink (or a reader) will receive
copies of the same events.  

Events read from a sink must be acknowledged by the client. Pulse automatically
requeues events added to a sink that have been read by a node but not
acknowledged.

Creating a sink is as simple as:

[![Single Sink](../snippets/single-sink.png)](../examples/streaming/single-sink/main.go#L37-L56)

Note a couple of differences with the reader example above:

- Sinks are given a name during creation, multiple nodes using the same name
  share the same stream cursor.
- Events are acknowledged using `sink.Ack`. This provides an at-least-once
  delivery guarantee where unacknowledged events are automatically re-queued.

```mermaid
%%{init: {'themeVariables': { 'background': '#282828', 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    main
    Stream
    Sink
    main-->|Add|Stream
    Stream-.->|Event|Sink
    Sink-.->|Event|main
    main-->|Ack|Sink

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class main userCode;
    class Stream,Sink pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

As with readers, multiple sinks can be created for the same stream. Copies of
the same event are distributed among all sinks.

[![Multi Sink](../snippets/multi-sink.png)](../examples/streaming/multi-sinks/main.go#L58-L79)

```mermaid
%%{init: {'themeVariables': { 'background': '#282828', 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    main-->|Add 1, 2|Stream
    Sink-.->|Events 1, 2|main
    main-->|Ack 1, 2|Sink
    Sink2-.->|Event 2|main
    main-->|Ack 2|Sink2
    Stream-.->|Events 1, 2|Sink
    Stream-.->|Event 2|Sink2[Other Sink]

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class main userCode;
    class Stream,Sink,Sink2 pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 4 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 5 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 6 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

## Reading from multiple streams

Readers and sinks can also read concurrently from multiple streams:

[![Multi Stream](../snippets/multi-stream.png)](../examples/streaming/multi-streams/main.go#L61-62)

```mermaid
%%{init: {'themeVariables': { 'background': '#282828', 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    main-->|Add 1|Stream
    main-->|Add 2|Stream2[Other Stream]
    Sink-.->|Event 1|main
    Sink-.->|Event 2|main
    main-->|Ack 1|Sink
    main-->|Ack 2|Sink
    Stream-.->|Event 1|Sink
    Stream2-.->|Event 2|Sink

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class main userCode;
    class Stream,Stream2,Sink pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 4 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 5 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 6 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

`AddStream` can be called at any time to add new streams to a reader or a sink.
Streams can also be removed using `RemoveStream`.

[![Remove Stream](../snippets/remove-stream.png)](../examples/streaming/multi-streams/main.go#L87-L91)

## Pub/Sub

Streams supports a flexible pub/sub mechanism where events can be attached to
topics and readers or sinks can define simple or custom matching logic.

[![Pub/Sub](../snippets/pub-sub.png)](../examples/streaming/pub-sub/main.go#L31-L35)

```mermaid
%%{init: {'themeVariables': { 'background': '#282828', 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart RL
    main-->|Add 1|Topic
    main-->|Add 2|Topic2
    subgraph Stream
        Topic2[Other Topic]
        Topic
    end
    Topic-.->|Event 1|Sink
    Topic2-.->|Event 2|Sink
    Sink-.->|Event 1|main
    Sink-.->|Event 2|main
    main-->|Ack 1|Sink
    main-->|Ack 2|Sink

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class main userCode;
    class Stream,Topic,Topic2,Sink pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 4 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 5 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 6 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

Topics can be matched using their name as in the example above or using complex
patterns. For example:

[![Pub/Sub](../snippets/pub-sub-pattern.png)](../examples/streaming/pub-sub/main.go#L76-L79)

> Note: Event filtering is done client-side in the sink or reader and does not
> affect the underlying stream. This means that events are still stored in the
> stream and can be consumed by other sinks.

## Examples

The [examples](../examples/streaming) directory contains a number of examples
that demonstrate the basic usage of the `streaming` package.