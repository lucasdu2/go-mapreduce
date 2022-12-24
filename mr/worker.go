package mapreduce

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type TaskRequest struct {
	workerIndex       int      // Index of worker
	prevTaskIndex     int      // Index of previous task
	prevTaskCompleted bool     // Indicate if previously assigned task completed
	outputFiles       []string // Return completed task output files
}

type Worker struct {
	workerIndex int // Identifying index of worker
	R           int // Number of Reduce tasks (used in partitioning)
	mapFunc     func(string, *map[string]string) error
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
		// NOTE: We will call the temp file directory "workbench"
		fp, err := os.CreateTemp("workbench", filename)
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
		outFiles = outFiles.append(fp.Name())
	}
	return outFiles, nil
}

// runMap takes in the data and task index for a Map task and runs the
// application-specified Map function on the data. It writes the results to
// a set of intermediate files, then returns that set of intermediate files in
// a slice of strings.
func (w *Worker) runMap(fname string, taskIndex int) ([]string, error) {
	// Create outFiles string slice
	var outFiles []string
	// Read data from file
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	// Convert byte array to string
	data = string(data)
	// Run Map function on data
	// Create dict to store intermediate key, value pairs
	storedict := make(map[string]string)
	err = w.mapFunc(data, &storedict)
	if err != nil {
		return nil, err
	}

	// Sort keys into R partitions using partitioning function
	// partitionToKVs maps a partition index to all the key, value pairs that
	// are partitioned to it. key, value pairs will be converted to strings of
	// form: "key,value"
	partitionToKVs := make(map[int][]string)
	var sb strings.Builder
	for key, value := range storedict {
		p := w.partFunc(key, w.R)
		sb.Reset()
		sb.WriteString(key)
		sb.WriteString(",")
		sb.WriteString(value)
		kvString := sb.String()
		partitionToKVs[p] = append(partitionToKVs[p], kvString)
	}
	// Write resulting data to intermediate files
	outFiles, err := w.writeIntermediateFiles(taskIndex, partitionToKVs)
	if err != nil {
		return nil, err
	}
	return outFiles, nil
}

func (w *Worker) addFileValuesToMap(fname string,
	collectedKVs map[string][]string) error {
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
		kvSplit = strings.Split(kv, ",")
		key := kvSplit[0]
		values := kvSplit[1]
		if _, ok := collectedKVs[key]; ok {
			// TODO: Figure out how to format these values...
			// Are the values from Map always going to be single values or is it
			// possible to have a list? We need to formalize this to minimize
			// surprises.
			collectedKVs[key] = collected // TODO
		} else {

		}

	}

}

func (w *Worker) runReduce(fnames []string, taskIndex int) error {
	// Read data from each file and sort by intermediate keys
	// Create map from each key to collected set of values across all input files
	collectedKVs := make(map[string][]string)
	for _, fname := range fnames {
		// Read each key from file and add its set of values to the map

	}

	// Run Reduce on each key
	// Write output to temporary file
	// Attempt atomic rename to final output file name
	return
}

// Run Worker execution flow
func (w *Worker) Run() {
	// Run heartbeat in the background as a goroutine
	return
}
