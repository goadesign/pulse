# Replicated Map Multi-Node Example

This example demonstrates how to use a replicated map across multiple nodes. 

## Running the Example

To run the example, follow these steps:

1. Make sure you have Go installed on your system. You can download and install
   it from the official Go website: https://golang.org/.

2. Make sure you have docker compose installed on your system. You can download
   and install it from the official Docker website: https://docs.docker.com/compose/install/.

3. Open a terminal or command prompt.

4. Clone the `goadesign/ponos` repository by running the following command:
   ```
   git clone https://github.com/goadesign/ponos.git
   ```

5. Change into the `examples/rmap/multi-node` directory:
   ```
   cd ponos/examples/rmap/multi-node
   ```

6. Install the required dependencies by running the following command:
   ```
   go get github.com/redis/go-redis/v9 goa.design/ponos/rmap
   ```

7. Build the Go program by executing the following command:
   ```
   go build
   ```

8. Run the program without any command-line arguments to listen for updates. Use
   the following command:
   ```
   ./multi-node
   ```

   This will start the program, and it will wait for updates on the replicated map.

9. Open another terminal and run the program with the "write" argument. Use the following command:
   ```
   ./multi-node write
   ```

   This will reset the map and write `numitems` key-value pairs to the map.

10. The program will print "waiting for updates..." and wait until it receives
    all the updates or keys. Once it receives `numitems` updates, it will print
    the final content of the map.


That's it! You have successfully run the example code. You can experiment with
different configurations, change the number of items, or modify the code to suit
your needs.

## How It Works

Here's a breakdown:

* The Redis client is created by specifying the Redis server's address and
  password (if available). This client will be used to communicate with the
  Redis server.

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
```

* The code checks if the Redis server is running and accessible by sending a
  Ping command. If the Ping fails, the program panics.

```go
if err := rdb.Ping(ctx).Err(); err != nil {
	panic(err)
}
```

* The replicated map named "my-map" is joined using the rmap.Join function,
  which takes the Redis client as an argument. This function returns a
  replicated map object (`m`) and an error (if any).

```go
m, err := rmap.Join(ctx, "my-map", rdb)
```

* If the command-line argument "write" is provided, the code resets the map
  using `m.Reset`. This clears any existing data in the map. Then, a loop is
  used to write numitems key-value pairs to the map using `m.Set`.

```go
if len(os.Args) > 1 && os.Args[1] == "write" {
	// Reset the map
	if err := m.Reset(ctx); err != nil {
		panic(err)
	}

	// Send updates
	for i := 0; i < numitems; i++ {
		if _, err := m.Set(ctx, "foo-"+strconv.Itoa(i+1), "bar"); err != nil {
			panic(err)
		}
	}
}
```

* A goroutine is started to listen for updates on the map. It subscribes to the
  map using m.Subscribe().

```go
go func() {
	defer wg.Done()
	c := m.Subscribe()
```

* Inside the goroutine, a loop is entered to wait for updates by receiving from
  the channel c. Each time an update is received, the code prints the number of
  keys in the map using m.Len(). If the number of keys reaches numitems, the
  loop is broken.

```go
for range c {
	fmt.Printf("%q: %v keys\n", m.Name, m.Len())
	if m.Len() == numitems {
		break
	}
}
```

* The main goroutine prints "waiting for updates..." and waits for the updates
  to be received by using `wg.Wait()`. This ensures that all updates are received
  before proceeding.

* Finally, the code prints the final content of the map using `m.Map()`. This
  function returns a snapshot of the current key-value pairs in the map.

In summary, this code demonstrates how to use the rmap package to create a
replicated map using Redis as the underlying storage. It shows how to write data
to the map, subscribe to updates, and retrieve the final content of the map. The
package provides a convenient way to build fault-tolerant distributed systems by
leveraging Redis's replication capabilities.