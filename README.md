# Ponos

Ponos consists of a set of packages that enable event driven distributed
architectures at scale. Each package is designed to be used independently but
they can also be combined to implement more complex architectures.

## Replicated Maps

Replicated maps provide a mechanism for sharing data across distributed nodes
and receiving events when the data changes.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    A[Node A]
    B[Node B]
    Map[Replicated Map]
    A-->|Set|Map
    Map-.->|Update|B

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef ponos fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class A,B userCode;
    class Map ponos;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

See the [rmap package README](rmap/README.md) for more details.

## Streaming

Ponos streams provide a flexible mechanism for routing events across a fleet of
microservices. Streams can be used to implement pub/sub, fan-out and fan-in
topologies.

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

See the [streaming package README](streaming/README.md) for more details.

## Dedicated Worker Pool

Ponos builds on top of [replicated maps](rmap/README.md) and
[streaming](streaming/README.md) to implement a dedicated worker pool where jobs
are dipatched to workers based on their key and a consistent hashing algorithm.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    A[Job Producer]
    subgraph Pool[Pool Node]
        Sink
    end
    subgraph Worker[Pool Node]
        Reader
        B[Worker]
    end
    A-->|Job+Key|Sink
    Sink-.->|Job|Reader
    Reader-.->|Job|B

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef ponos fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class A,B userCode;
    class Pool,Sink,Reader,Worker ponos;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
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
