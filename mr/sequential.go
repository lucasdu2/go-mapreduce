package mr

import (
	"fmt"
	"log"
	"os"
	"plugin"
	"sort"
	"strings"
)

func runSequentialMap(
	fname string,
	intermediateData map[string][]string,
	mapFunc plugin.Symbol,
) error {
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

func runSequentialReduce(
	partitionIndex int,
	keys []string,
	intermediateData map[string][]string,
	redFunc plugin.Symbol,
) error {
	// Convert Reduce function from symbol
	reducer := redFunc.(func(string, []string, *os.File) error)
	// Create output file in append mode
	outputName := fmt.Sprintf("sequential-outputs/mr-out-%v", partitionIndex)
	fp, err := os.OpenFile(outputName, os.O_APPEND|os.O_WRONLY|os.O_CREATE,
		0644)
	if err != nil {
		return err
	}
	defer fp.Close()
	// Sort keys to ensure ordering guarantees (see 4.2 of reference paper)
	sort.Strings(keys)
	for _, k := range keys {
		err := reducer(k, intermediateData[k], fp)
		if err != nil {
			return err
		}
	}
	return nil
}

func SequentialRun(
	inputFile string,
	mapFunc, redFunc, partFunc plugin.Symbol,
	numOutputs int,
	killChan chan int,
) {
	// Handle interrupts
	go func() {
		intermediateData := make(map[string][]string)
		// Take in input file and run Map function on the data sequentially
		log.Println("Starting sequential Map")
		err := runSequentialMap(inputFile, intermediateData, mapFunc)
		if err != nil {
			log.Panic(err)
		}
		// Partition intermediate data into numOutputs pieces
		partitioner := partFunc.(func(string, int) int)
		partitionedKeys := make([][]string, numOutputs)
		for k := range intermediateData {
			p := partitioner(k, numOutputs)
			partitionedKeys[p] = append(partitionedKeys[p], k)
		}
		// Create directory to store outputs from sequential MapReduce
		err = os.Mkdir("sequential-outputs", 0755)
		if err != nil {
			if strings.Contains(err.Error(), "file exists") {
				log.Println("Directory \"sequential-outputs\" already exists " +
					"from previous run, please remove and try again")
			}
			log.Panic(err)
		}
		// Run Reduce function on each partition sequentially
		log.Println("Starting sequential Reduce")
		for i := 0; i < numOutputs; i++ {
			err := runSequentialReduce(i, partitionedKeys[i], intermediateData,
				redFunc)
			if err != nil {
				log.Panic(err)
			}
		}
		killChan <- 1
	}()
	<-killChan
	log.Println("Exiting sequential MapReduce")
}
