package main

import (
	"flag"
	"plugin"
)

func errCheck(e error) {
	if e != nil {
		panic(e)
		// log.Fatal(e)
	}
}

func main() {
	// Declare error variable
	var err error

	// Parse command line arguments
	inputFile := flag.String("input", "mrapps/wc-in-A.txt", "input filename")
	appFunctions := flag.String("functions", "mrapps/wc.so",
		"plugin filename containing all application-specific functions: "+
			"Map, Reduce, InputSplitter, Partitioner")
	numWorkers := flag.Int("workers", 4, "number of workers to spawn")
	m := flag.Int("m", 32, "number of Map tasks to create")
	r := flag.Int("r", 8, "number of Reduce tasks to create")
	flag.Parse()

	// TODO: Consider saving all intermediate outputs to a temporary directory
	// to aid in debugging.

	// Load application-specific functions at runtime
	p, err := plugin.Open(*appFunctions)
	errCheck(err)
	mapFunc, err := p.Lookup("Map")
	errCheck(err)
	redFunc, err := p.Lookup("Reduce")
	errCheck(err)
	splitter, err := p.Lookup("InputSplitter")
	errCheck(err)
	partitioner, err := p.Lookup("Partitioner")
	errCheck(err)

	// Split input using application-specified InputSplitter function
	err = splitter.(func(string, int) error)(*inputFile, *m)
	errCheck(err)

	// Spawn coordinator and worker programs
	// NOTE: Because we do not have access to an actual cluster of servers, we
	// will simply be starting the coordinator and workers on a single local
	// machine as concurrent goroutines that communicate using RPCs

	// Combine all R Reduce outputs into single output file

}
