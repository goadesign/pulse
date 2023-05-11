# Replicated Map

Replicated maps provide a mechanism for sharing data across a fleet of
microservices.

## Overview

Ponos replicated maps leverage Redis hashes and pub/sub to maintain replicated
in-memory copies of a map across multiple nodes. Any change to the map is
automatically replicated to all nodes and results in a notification that can be
used to trigger actions.

Upon joining a replicated map the node receives an up-to-date snapshot of its
content. The replicated map then guarantees that any change to the map results
in a notification no matter when the client calls the `Subscribe` method.


## Usage

### Creating a Replicated Map

To create a replicated map you must provide a name and a Redis client. The name
is used to namespace the Redis keys and pub/sub channels used by the map. The
map should later be closed to free up resources.

```go
package main

import (
        "context"

        "github.com/redis/go-redis/v9"
        "goa.design/ponos/rmap"
)

func main() {
        // Create a Redis client
        client := redis.NewClient(&redis.Options{
            Addr: "localhost:6379",
        })
    
        // Join or create a replicated map
        m, err := rmap.Join(context.Background(), "my-map", client)
        if err != nil {
            panic(err)
        }

        // ... use the map

        // Cleanup when done
        if err := m.Close(); err != nil {
            panic(err)
        }
}
```

### Reading and Writing to the Map

The `Map` method returns a copy of the current map. The `Get` method returns the
value for a given key. The `Set` method sets the value for a given key. 

```go
        // Print the current contents of the map
        fmt.Println(m.Map())

        // Add a new value, old contains the previous value for the key
        old, err := m.Set(context.Background(), "foo", "bar")

        // Retrieve a value
        val, ok := m.Get("foo") // ok is true if the key exists
```

Additionally the `Map` struct exposes convenience methods to manage values that behave
like slices and values that behave like counters:

```go
        // Append new string values to the slice, returns the new slice
        res, err := m.AppendValues(context.Background(), "my-array", "value1", "value2", "value3")

        // RemoveValues removes value from the slice, returns the new slice
        res, err := m.RemoveValues(context.Background(), "my-array", "value1", "value2")

        // Increment a counter by 42, they key must hold a valid string
        // representation of an integer
        res, err := m.Inc(context.Background(), "foo", 42)
```

## When to Use Replicated Maps

Replicated maps being stored in memory are not suitable for large data sets. They
are also better suited for read-heavy workloads as reads are local but writes
require a roundtrip to Redis.

A good use case for replicated maps is metadata or configuration information that
is shared across a fleet of microservices. For example a replicated map can be
used to share the list of active users across a fleet of microservices. The
microservices can then use the replicated map to check if a user is active
without having to query a database.

## Example

The following example creates a replicated map and then starts a goroutine that
listens to notifications. The main goroutine then sets 100 keys and waits for
the notifications to be received.

```go
package main

import (
        "context"
        "fmt"
        "strconv"
        "sync"

        "github.com/redis/go-redis/v9"
        "goa.design/ponos/rmap"
)

func main() {
        // Create a Redis client
        client := redis.NewClient(&redis.Options{
                Addr: "localhost:6379",
                Password: os.Getenv("REDIS_PASSWORD"),
        })
    
        // Join or create a replicated map
        ctx := context.Background()
        m, err := rmap.Join(ctx, "my-map", client)
        if err != nil {
                panic(err)
        }

        // Start a goroutine to listen for updates
        numUpdates := 100
        var wg sync.WaitGroup
        wg.Add(1)
        go func() {
                defer wg.Done()
                for range m.C {
                        if m.Len() == numUpdates {
                                // We received all the updates
                                return
                        }
                }
        }()

        // Send a few updates
        for i := 0; i < numUpdates; i++ {
                m.Set(ctx, "foo-" + strconv.Itoa(i+1), "bar")
        }

        // Wait for the updates to be received
        wg.Wait()
}
```
