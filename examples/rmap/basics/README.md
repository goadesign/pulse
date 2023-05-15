# Replicated Map Basics Example

This example demonstrates the basic usage of a replicated map using the `rmap` package from the `goadesign/ponos` repository.

## Running the Example

To run the example, follow these steps:

1. Make sure you have Go installed on your system. You can download and install it from the official Go website: https://golang.org/.

2. Open a terminal or command prompt.

3. Clone the `goadesign/ponos` repository by running the following command:
   ```
   git clone https://github.com/goadesign/ponos.git
   ```

4. Change into the `examples/rmap/basics` directory:
   ```
   cd ponos/examples/rmap/basics
   ```

5. Install the required dependencies by running the following command:
   ```
   go get github.com/redis/go-redis/v9 goa.design/ponos/rmap
   ```

6. Build the Go program by executing the following command:
   ```
   go build
   ```

7. Run the program using the following command:
   ```
   ./basics
   ```

   This will execute the program and demonstrate the basic operations on the replicated map.

That's it! You have successfully run the example code. The program will showcase how to create a replicated map, set values, retrieve values, and perform other basic operations.

## How It Works

Here's a breakdown of the code:

* The program imports the necessary packages, including `github.com/redis/go-redis/v9` for the Redis client and `goa.design/ponos/rmap` for the replicated map functionality.

* Inside the `main()` function, a Redis client is created using `redis.NewClient()` with the Redis server address and password (if available).

* The code checks if the Redis server is running and accessible by sending a Ping command using `rdb.Ping()`. If the Ping fails, the program panics.

* The replicated map named "my-map" is joined using `rmap.Join()`, which takes the Redis client as an argument. If any error occurs during joining the map, the program panics.

* A channel `c` is created to receive updates from the replicated map using `m.Subscribe()`.

* The map is reset using `m.Reset()` to clear any existing data in the map.

* Key-value pairs are set in the replicated map using `m.Set()` for a string value and `m.AppendValues()` for a list value.

* Elements are removed from the list using `m.RemoveValues()`.

* An integer value is incremented using `m.Inc()`.

* A goroutine is started to listen for updates on the map. Inside the goroutine, it waits for updates by receiving from the channel `c`. It prints the number of keys in the map using `m.Len()` and breaks the loop when the map has 3 keys.

* The main goroutine prints "waiting for updates..." and waits for the updates to be received using `sync.WaitGroup` before proceeding.

* Finally, the program prints the final content of the map using `m.Map()`.

The code demonstrates the fundamental operations of creating a replicated map, setting values, retrieving values, and performing basic operations on it. It provides a starting point for understanding the basic usage of the `rmap` package.