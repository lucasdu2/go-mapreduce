package mr

import (
	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Worker struct {
	workerIndex int // Identifying index of worker
	r           int // Number of Reduce tasks (used in partitioning)
	mapFunc     func(string, map[string][]string) error
	redFunc     func(string, []string, *os.File) error
	partFunc    func(string, int) int
}

// writeIntermediateFiles is a helper function for runMap. It takes in the index
// of the task and a map of partition indexes to all the key, value pairs that
// are in a particular partition. Then it actually writes the data to intermediate
// files and returns a slice of strings containing the names of the intermediate
// files.
func (w *Worker) writeIntermediateFiles(taskIndex int,
	partitionToKVs map[int][]string) ([]string, error) {
	var outFiles []string
	var sb strings.Builder
	for partitionIndex, kvs := range partitionToKVs {
		// Specify proper filename
		// NOTE: All intermediate files will be of the form workerN-X-Y, where X
		// is the index of the Map task, Y is the index of a Reduce task, and
		// N is the index of the worker that produced the file. Additionally, we
		// expect all intermediate files to be placed within a directory called
		// workbench.
		sb.Reset()
		sb.WriteString("worker")
		sb.WriteString(strconv.Itoa(w.workerIndex))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(taskIndex))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(partitionIndex))
		filename := sb.String()
		// Add all key, value pairs to appropriate intermediate file
		// NOTE: We will call the intermediate file directory "workbench"
		fp, err := os.Create("workbench/" + filename)
		if err != nil {
			return nil, err
		}
		defer fp.Close()
		// NOTE: CreateTemp only opens the file for reading/writing and we can't
		// add an option to open it in append mode, so we cannot write the
		// key, value pairs one by one. Instead, we construct the entire string
		// containting all key, value pairs we want to write, then write to file
		// with WriteString.
		sb.Reset()
		for _, kv := range kvs {
			sb.WriteString(kv)
			sb.WriteString("\n")
		}
		_, err = fp.WriteString(sb.String())
		if err != nil {
			return nil, err
		}
		// Add intermediate file name to outFiles
		outFiles = append(outFiles, fp.Name())
	}
	return outFiles, nil
}

// runMap takes in the data and task index for a Map task and runs the
// application-specified Map function on the data. It writes the results to
// a set of intermediate files, then returns that set of intermediate files in
// a slice of strings.
func (w *Worker) runMap(fname string, taskIndex int) ([]string, error) {
	log.Printf("Worker %v starting Map Task %v", w.workerIndex, taskIndex)
	// Create outFiles string slice
	var outFiles []string
	// Read data from file
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	// Convert byte array to string
	dataString := string(data)
	// Run Map function on data
	// Create dict to map intermediate keys to list of associated values
	storedict := make(map[string][]string)
	err = w.mapFunc(dataString, storedict)
	if err != nil {
		return nil, err
	}

	// Sort keys into R partitions using supplied partitioning function
	// partitionToKVs maps a partition index to all the key, value pairs that
	// are partitioned to it. key, value pairs will be converted to strings of
	// form "key,value" and will be written in this form to intermediate files.
	// NOTE: Each "key,value" line in an intermediate file will only map one key
	// to one value. A key could be mapped to multiple values; they will just be
	// on separate lines.
	partitionToKVs := make(map[int][]string)
	var sb strings.Builder
	// NOTE: "values" is a slice of strings that could contain multiple values
	// corresponding to a key. We need to split these up into individual key,
	// value pairs to match the expected format within intermediate files.
	// Intermediate files should always contain one key, value pair on each line;
	// if a key maps to multiple values, they will be on separate lines (see
	// previous note).
	for key, values := range storedict {
		p := w.partFunc(key, w.r)
		sb.Reset()
		sb.WriteString(key)
		sb.WriteString(",")
		prefix := sb.String()
		for _, value := range values {
			sb.Reset()
			sb.WriteString(prefix)
			sb.WriteString(value)
			// Append each individual "key,value" pair (one key to one value) to
			// the string slice associated with the appropriate partition.
			partitionToKVs[p] = append(partitionToKVs[p], sb.String())
		}
	}
	// Write resulting data to intermediate files
	outFiles, err = w.writeIntermediateFiles(taskIndex, partitionToKVs)
	if err != nil {
		return nil, err
	}
	return outFiles, nil
}

func (w *Worker) sortByKey(fname string, sorted map[string][]string) error {
	fp, err := os.Open(fname)
	if err != nil {
		return err
	}
	fileScanner := bufio.NewScanner(fp)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		// Handle any errors during scan
		if err := fileScanner.Err(); err != nil {
			return err
		}
		kv := fileScanner.Text()
		// kv should be a string of the form "key,value"; this is the form
		// specified when we write to intermediate files in runMap()
		kvSplit := strings.Split(kv, ",")
		key := kvSplit[0]
		value := kvSplit[1]
		sorted[key] = append(sorted[key], value)
	}
	return nil
}

func (w *Worker) runReduce(fnames []string, TaskIndex int) error {
	// Read data from each file and sort by intermediate keys
	// Create map from each key to collected set of values across all input files
	collectedKVs := make(map[string][]string)
	for _, fname := range fnames {
		// Read each key from file and add its set of values to the map
		err := w.sortByKey(fname, collectedKVs)
		if err != nil {
			return err
		}
	}
	// Sort keys to ensure ordering guarantees (see 4.2 of reference paper)
	sortedKeys := make([]string, len(collectedKVs))
	i := 0
	for k, _ := range collectedKVs {
		sortedKeys[i] = k
		i++
	}
	sort.Strings(sortedKeys)
	// Run Reduce on list of values associated with each key and write output
	// to temporary file
	var sb strings.Builder
	sb.Reset()
	sb.WriteString("out-")
	sb.WriteString(strconv.Itoa(TaskIndex))
	sb.WriteString("-worker")
	sb.WriteString(strconv.Itoa(w.workerIndex))
	sb.WriteString("-*")
	filenamePrefix := sb.String()
	// Create temp file in workbench for output file
	tempf, err := os.CreateTemp("workbench", filenamePrefix)
	if err != nil {
		return err
	}
	// Get exact name of temp file
	tf := tempf.Name()
	// Ensure the temp file pointer gets closed
	defer tempf.Close()
	// Re-open output file in append mode
	fp, err := os.OpenFile(tf, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()
	for _, k := range sortedKeys {
		err = w.redFunc(k, collectedKVs[k], fp)
		if err != nil {
			return err
		}
	}
	// Attempt atomic rename to final output file name
	sb.Reset()
	sb.WriteString("workbench/mr-out-")
	sb.WriteString(strconv.Itoa(TaskIndex))
	filenameFinal := sb.String()
	// NOTE: os.Rename is guaranteed to be atomic only on Unix systems.
	// Additionally, it appears to be best practice to only rename within a
	// the same directory (which is why we also initially place the final
	// output file in the "workbench" temp file directory).
	err = os.Rename(tf, filenameFinal)
	if err != nil {
		return err
	}
	return nil
}
