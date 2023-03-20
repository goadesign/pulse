# Ponos

Ponos enables event driven distributed architectures by providing scalable
event streaming and tenanted worker pools based on Redis. 

## Replicated Maps

Replicated maps provide a mechanism for sharing data across a fleet of
microservices and receiving events when the data changes.

Replicated maps consist of an in-memory map of strings with copies shared across
all participating processes. Any process can update the replicated map, updates
are propagated within milliseconds across all processes. Replicated maps are
implemented using Redis hashes and pub/sub.

```mermaid
flowchart TD
    subgraph Processes [Client Processes]
        direction LR
        H[fa:fa-microchip Process A]
        G[fa:fa-microchip Process B]
    end
    Map[[fa:fa-map-o Replicated Map]]
    H-->|fa:fa-map-o Set|Map
    Map-.->|fa:fa-map-o Update|G
    G-->|fa:fa-map-o Get|Map
```

See the [replicated package README](replicated/README.md) for more details.

## Streams

Ponos streams provide a flexible mechanism for routing events across a fleet of
microservices. Event sinks can subscribe to multiple streams and consume events
concurrently. Streams can be used to implement pub/sub, fan-out and fan-in
topologies.

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
    subgraph Stream
        direction TB
        subgraph Topic
            direction TB
            A[fa:fa-bolt Event 1]-.-B[fa:fa-bolt Event n]
        end
        subgraph Topic2 [Other topic]
            direction TB
            A2[fa:fa-bolt Event 1]-.-B2[fa:fa-bolt Event m]
        end
    end
    subgraph Stream2 [Other stream]
        direction TB
        C[fa:fa-bolt Event 1]-.-D[fa:fa-bolt Event n]
    end
    subgraph Sink [Fan-in sink]
        direction LR
        E[fa:fa-microchip Process]-.-F[fa:fa-microchip Process]
    end
    subgraph Sink2 [Other sink]
        direction LR
        E2[fa:fa-microchip Process]-.-F2[fa:fa-microchip Process]
    end
    Producers-->|fa:fa-bolt Add event|Topic
    Producers-->|fa:fa-bolt Add event|Topic2
    Producers2-->|fa:fa-bolt Add event|Stream2
    Topic-->|fa:fa-bolt Events|Sink
    Topic2-->|fa:fa-bolt Events|Sink
    Stream2-->|fa:fa-bolt Events|Sink
    Topic2-->|fa:fa-bolt Events|Sink2
```

See the [streams package README](streams/README.md) for more details.

## Tenanted Worker Pool

Ponos builds on top of the [replicated](replicated/README.md) and
[streams](streams/README.md) packages to implement a tenanted worker pool where
jobs are distributed to worker groups based on a key.

```mermaid
flowchart TD
    subgraph Producers
        direction LR
        H[fa:fa-microchip Process]-.-G[fa:fa-microchip Process]
    end
    subgraph Pool
        direction TB
        subgraph Worker [Worker Group A]
            direction TB
            A[fa:fa-tasks Job keyed 1]-.-B[fa:fa-tasks Job keyed n]
        end
        subgraph Worker2 [Worker Group B]
            direction TB
            A2[fa:fa-tasks Job keyed n+1]-.-B2[fa:fa-tasks Job keyed m]
        end
    end
    Producers-->|fa:fa-tasks Add keyed job|Pool
```

See the [pool package README](pool/README.md) for more details.

## License

Ponos is licensed under the MIT license. See [LICENSE](LICENSE) for the full
license text.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting patches and the
contribution workflow.

## Code of Conduct

This project adheres to the Contributor Covenant [code of conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code. Please report unacceptable
behavior to [ponos@goa.design](mailto:ponos@goa.design).

## Credits

Ponos was originally created by [Raphael Simon](@raphael).