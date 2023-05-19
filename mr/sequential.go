package mr

import (
	"log"
	"os"
	"plugin"
	"sort"
)

func runMap(fname string, intermediateData map[string][]string,
	mapFunc plugin.Symbol) error {

	// Convert Map function from symbol
	mapper := mapFunc.(func(string, map[string][]string) error)
	// Read data from file
	data, err := os.ReadFile(fname)
	if err != nil {
		return err
	}
	// Convert byte array to string
	dataString := string(data)
	// Run Map function on data
	err = mapper(dataString, intermediateData)
	if err != nil {
		return err
	}
	return nil
}

func runReduce(intermediateData map[string][]string,
	redFunc plugin.Symbol) error {

	// Convert Reduce function from symbol
	reducer := redFunc.(func(string, []string, *os.File) error)
	outputName := "mr-out-sequential"
	// Create output file in append mode
	fp, err := os.OpenFile(outputName, os.O_APPEND|os.O_WRONLY|os.O_CREATE,
		0644)
	if err != nil {
		return err
	}
	defer fp.Close()
	// Sort keys to ensure ordering guarantees (see 4.2 of reference paper)
	sortedKeys := make([]string, len(intermediateData))
	i := 0
	for k := range intermediateData {
		sortedKeys[i] = k
		i++
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		err := reducer(k, intermediateData[k], fp)
		if err != nil {
			return err
		}
	}
	return nil
}

func SequentialRun(
	inputFile string,
	mapFunc, redFunc plugin.Symbol,
	killChan chan int,
) {
	// Handle interrupts
	go func() {
		intermediateData := make(map[string][]string)
		// Take in input file and run Map function on the data sequentially
		log.Println("Starting sequential Map")
		err := runMap(inputFile, intermediateData, mapFunc)
		if err != nil {
			log.Panic(err)
		}
		// Run Reduce function on each key sequentially and output results
		log.Println("Starting sequential Reduce")
		err = runReduce(intermediateData, redFunc)
		if err != nil {
			log.Panic(err)
		}
		killChan <- 1
	}()
	<-killChan
	log.Println("Exiting sequential MapReduce")
}
