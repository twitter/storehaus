# storehaus-benchmark

[jmh](http://openjdk.java.net/projects/code-tools/jmh/)-based Benchmarks for Storehaus stores.

## Usage

Run the following commands from the top-level Storehaus directory:

    $ ./sbt   # <<< enter sbt REPL
    > project storehaus-benchmark

Now you can run the following commands from within the sbt REPL:

    # List available benchmarks
    > jmh:run -l

    # Run a particular benchmark
    > jmh:run -t1 -f1 -wi 2 -i 3 .*WriteThroughCacheBenchmark.*

    # Run all benchmarks
    > jmh:run .*

These options tell JMH to run the benchmark with 1 thread (`-t1`), 1 fork (`-f1`), 2 warmup iterations and 3 real iterations. You can find further details in the [sbt-jmh](https://github.com/ktoso/sbt-jmh) documentation.
