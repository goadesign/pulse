# Streaming Examples

The examples in the subdirectories demonstrate the basic usage of the `streaming` package from the `goadesign/pulse` repository.

## Running the Examples

To run the examples, follow these steps:

1. Make sure you have Go installed on your system. You can download and install it from the official Go website: https://golang.org/.

2. Open a terminal or command prompt.

3. Clone the `goadesign/pulse` repository by running the following command:
   ```
   git clone https://github.com/goadesign/pulse.git
   ```

4. Change into the example directory (e.g. `examples/streaming/single-reader`):
   ```
   cd pulse/examples/streaming/single-reader
   ```
   ```

5. Install the required dependencies by running the following command:
   ```
   go get github.com/redis/go-redis/v9 goa.design/pulse/rmap
   ```

6. Build the Go program by executing the following command:
   ```
   go build
   ```

7. Run the program using the following command:
   ```
   ./single-reader
   ```

This will execute the program and demonstrate the basic operations on streaming.
