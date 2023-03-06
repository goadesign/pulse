# Ponos

Ponos enables event driven distributed architectures by providing scalable
event streaming and tenanted worker pools based on Redis. 

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
    Stream2-->|fa:fa-bolt Events|Sink
    Topic2-->|fa:fa-bolt Events|Sink2
```

See [Streams](streams/README.md) for more details.

## Tenanted Workers

Ponos builds on top of the [streams](streams/README.md) package to implement a tenanted worker pool.

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