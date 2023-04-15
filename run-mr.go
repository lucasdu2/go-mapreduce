package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"plugin"

	// TODO: Need to export the mapreduce package with this name
	mr "github.com/lucasdu2/go-mapreduce/mr"
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

	// Create "workbench" directory for holding intermediate files
	err = os.Mkdir("workbench", 0755)
	errCheck(err)
	// TODO: Uncomment this later, commented out for testing
	// defer os.RemoveAll("workbench")

	// Spawn coordinator and worker programs
	// NOTE: Because we do not have access to an actual cluster of servers, we
	// will simply be starting the coordinator and workers on a single local
	// machine as concurrent goroutines that communicate using RPCs.
	for i := 0; i < *numWorkers; i++ {
		go mr.WorkerRun(i, *r, mapFunc, redFunc, partitioner)
	}
	// Create termination channel
	killChan := make(chan int, 1)
	// Handle manual interrupt of MapReduce operation
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		killChan <- 1
		log.Println("Shutting down coordinator, exiting MapReduce operation")
	}()
	mr.CoordinatorRun(*m, *r, *numWorkers, killChan)
	// Move all R Reduce outputs out from workbench directory
	// Combine all R Reduce outputs into single output file

}
