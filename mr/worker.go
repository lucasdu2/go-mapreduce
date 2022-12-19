package mapreduce

import (
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

func (w *Worker) runMap(fname string, taskIndex int, outFiles *[]string) error {
	// Read data from file
	data, err := os.ReadFile(fname)
	if err != nil {
		return err
	}
	// Convert byte array to string
	data = string(data)
	// Run Map function on data
	// Create dict to store intermediate key, value pairs
	storedict := make(map[string]string)
	err = w.mapFunc(data, &storedict)
	if err != nil {
		return err
	}

	// Sort keys into R partitions using partitioning function
	// partitionToKVs maps a partition index to all the key, value pairs that
	// are partitioned to it. key, value pairs will be converted to strings of
	// form: "key,value"
	var partitionToKVs map[int][]string
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
	for partitionIndex, kv := range partitionToKVs {
		// Specify proper filename
		// NOTE: All intermediate files will be of the form workerN-X-Y, where X
		// is the index of the Map task, Y is the index of a Reduce task, and
		// N is the index of the worker that produced the file. Additionally, we
		// expect all intermediate files to be placed within a directory called
		// workbench.
		sb.Reset()
		sb.WriteString("workbench/")
		sb.WriteString("worker")
		sb.WriteString(strconv.Itoa(w.workerIndex))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(taskIndex))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(partitionIndex))
		filename := sb.String()
		// Append key, value pairs to intermediate file
		os.OpenFile(filename, // TODO)

	}
	return
}

func (w *Worker) runReduce() error {
	// Read data from each file
	// Sort by intermediate keys
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
