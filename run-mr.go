package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"plugin"
	"strconv"
	"strings"

	mr "github.com/lucasdu2/go-mapreduce/mr"
)

func errCheck(e error) {
	if e != nil {
		log.Panic(e)
	}
}

// copyFile copies the contents of the file at srcpath to a regular file at
// dstpath. If dstpath already exists and is not a directory, the function
// truncates it. The function does not copy file modes or file attributes.
func copyFile(srcpath, dstpath string) (err error) {
	r, err := os.Open(srcpath)
	if err != nil {
		return err
	}
	defer r.Close() // ok to ignore error: file was opened read-only.

	w, err := os.Create(dstpath)
	if err != nil {
		return err
	}

	defer func() {
		e := w.Close()
		// Report the error from Close, if any.
		// But do so only if there isn't already
		// an outgoing error.
		if e != nil && err == nil {
			err = e
		}
	}()

	_, err = io.Copy(w, r)
	return err
}

func main() {
	// Declare error variable
	var err error

	// Parse command line arguments
	inputFile := flag.String("input", "mrapps/wc-in-A.txt", "input filename")
	appFunctions := flag.String("functions", "mrapps/wc.so",
		"plugin filename containing all application-specific functions: "+
			"Map, Reduce, InputSplitter, Partitioner")
	runSequential := flag.Bool("sequential", false,
		"run MapReduce sequentially (use as correctness/performance baseline)")
	numWorkers := flag.Int("workers", 4, "number of workers to spawn")
	m := flag.Int("m", 32, "number of Map tasks to create")
	r := flag.Int("r", 8, "number of Reduce tasks to create")
	cleanUpAfter := flag.Bool("clean", true,
		"clean up intermediate files upon MapReduce completion")
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

	// Handle sequential MapReduce
	// ---------------------------
	if *runSequential {
		mr.SequentialRun(*inputFile, mapFunc, redFunc, killChan)
		return
	}

	// Otherwise, execute regular parallel MapReduce
	// ---------------------------------------------
	// Split input using application-specified InputSplitter function
	err = splitter.(func(string, int) error)(*inputFile, *m)
	errCheck(err)

	// Create "workbench" directory for holding intermediate files
	err = os.Mkdir("workbench", 0755)
	if err != nil {
		if strings.Contains(err.Error(), "file exists") {
			log.Println("Working directory \"workbench\" already exists from " +
				"previous run, please remove and try again")
		}
		log.Panic(err)
	}
	if *cleanUpAfter {
		// Remove workbench directory
		defer os.RemoveAll("workbench")
		// Remove all input files (of form pg-*)
		defer func() {
			inputFiles, err := filepath.Glob("pg-*")
			errCheck(err)
			for _, f := range inputFiles {
				err = os.Remove(f)
				errCheck(err)
			}
		}()
	}

	// Spawn coordinator and worker programs
	// NOTE: Because we do not have access to an actual cluster of servers, we
	// will simply be starting the coordinator and workers on a single local
	// machine as concurrent goroutines that communicate using RPCs.
	for i := 0; i < *numWorkers; i++ {
		go mr.WorkerRun(i, *r, mapFunc, redFunc, partitioner)
	}
	mr.CoordinatorRun(*m, *r, *numWorkers, killChan)
	// Move all outputs from workbench directory into outputs directory
	err = os.Mkdir("outputs", 0755)
	if err != nil {
		if strings.Contains(err.Error(), "file exists") {
			log.Println("Output directory \"outputs\" already exists from " +
				"previous run, please remove and try again")
		}
		log.Panic(err)
	}
	for i := 0; i < *r; i++ {
		outfile := "mr-out-" + strconv.Itoa(i)
		err = copyFile("workbench/"+outfile, "outputs/"+outfile)
		errCheck(err)
	}
}
