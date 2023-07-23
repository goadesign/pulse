# pool Example

This example shows how to use the pool package to create a pool of workers. It has three parts:

1. The `worker` process registers a worker with the node and waits for jobs.
2. The `producer` process starts and stops two jobs. It also notifies the worker handling the second job.
3. The `scheduler` process starts runs a schedule that starts and stops jobs alternately.

## Running the example

To run the example, first make sure Redis is running locally. The `start-redis` script
in the scripts directory of the repository can be used to start Redis:

```bash
$ cd pulse
$ scripts/start-redis
```

Then, in two separate terminals, run the following commands:

```bash
$ source .env 
$ go run examples/pool/worker/main.go
```

The above start two workers that wait for jobs. Then, in separate terminals, run
the following commands:

```bash
$ source .env
$ go run examples/pool/producer/main.go
```

The above starts and stops two jobs. The first job is handled by the first worker,
and the second job is handled by the second worker.

Finally in the same terminal used above run the following command:

```bash
$ go run examples/pool/scheduler/main.go
```

The above starts a scheduler that starts and stops jobs alternately. The first job
is handled by the first worker, and the second job is handled by the second worker.

To stop the worker processes simply press `Ctrl-C` in the terminal where they are
running. Note that it takes a few seconds for the workers to stop (as they wait for
the Redis poll to return).

## Adding Verbose Logging

The three processes can be run with verbose logging by passing the `-v` flag:

```bash
$ go run examples/pool/worker/main.go -v
$ go run examples/pool/producer/main.go -v
$ go run examples/pool/scheduler/main.go -v
```

Verbose logging will print logs created by the `pulse` package to the terminal.