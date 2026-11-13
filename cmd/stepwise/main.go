// Command stepwise runs the demo, the chaos test and the benchmark.
//
//	stepwise demo     dashboard + supervised worker processes you can kill
//	stepwise chaos    run workflows while killing workers at random, then verify
//	stepwise bench    measure throughput and latency
//	stepwise worker   one worker process (started by demo and chaos)
package main

import (
	"fmt"
	"os"
)

func defaultDSN() string {
	if dsn := os.Getenv("STEPWISE_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://localhost:5433/stepwise?sslmode=disable"
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	args := os.Args[2:]
	var err error
	switch os.Args[1] {
	case "demo":
		err = runDemo(args)
	case "chaos":
		err = runChaos(args)
	case "bench":
		err = runBench(args)
	case "worker":
		err = runWorker(args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: stepwise <demo|chaos|bench|worker> [flags]   (use -h after a command for its flags)")
	os.Exit(2)
}
