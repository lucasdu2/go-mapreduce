package mr

import (
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
	err := os.Mkdir("outputs", 0755)
	if err != nil {
		return err
	}
	outputName := "outputs/mr-out-0"
	// Create output file in append mode
	fp, err := os.OpenFile(outputName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()
	// Sort keys to ensure ordering guarantees (see 4.2 of reference paper)
	sortedKeys := make([]string, len(intermediateData))
	i := 0
	for k, _ := range intermediateData {
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
	inputFiles []string,
	mapFunc, redFunc plugin.Symbol,
	kc chan int,
) {
	// TODO: Handle interrupts (probably use goroutine waiting on kc)
	intermediateData := make(map[string][]string)
	// Take in input files and run Map function on them sequentially
	for _, fname := range inputFiles {
		err := runMap(fname, intermediateData, mapFunc)
		if err != nil {
			panic(err)
		}
	}
	// Run Reduce function on each key sequentially and output results
	err := runReduce(intermediateData, redFunc)
	if err != nil {
		panic(err)
	}
}
