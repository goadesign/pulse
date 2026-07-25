# Streaming Examples

The examples in the subdirectories demonstrate the basic usage of the `streaming` package from the `goadesign/pulse` repository.

## Running the Examples

To run the examples, follow these steps:

1. Make sure you have Go installed on your system. You can download and install it from the official Go website: https://golang.org/.

2. Open a terminal or command prompt.

   The examples read `REDIS_ADDR` and `REDIS_PASSWORD`; from the repository
   root, `source .env` selects the default local Redis configuration.

3. Clone the `goadesign/pulse` repository by running the following command:
   ```bash
   git clone https://github.com/goadesign/pulse.git
   ```

4. Change into the example directory (e.g. `examples/streaming/single-reader`):
   ```bash
   cd pulse/examples/streaming/single-reader
   ```

5. Download the repository's pinned dependencies:
   ```bash
   go mod download
   ```

6. Build the Go program by executing the following command:
   ```bash
   go build
   ```

7. Run the program using the following command:
   ```bash
   ./single-reader
   ```

This will execute the program and demonstrate the basic operations on streaming.
The [`exact-publication`](exact-publication/main.go) example shows
deadline-owned `AddOnce` retries and read-only snapshots.
