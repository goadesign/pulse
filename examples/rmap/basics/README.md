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
