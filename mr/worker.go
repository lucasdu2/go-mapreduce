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
	// Create dict to map intermediate keys to list of associated values
	storedict := make(map[string][]string)
	err = w.mapFunc(data, storedict)
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
		p := w.partFunc(key, w.R)
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
	outFiles, err := w.writeIntermediateFiles(taskIndex, partitionToKVs)
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
		kvSplit = strings.Split(kv, ",")
		key := kvSplit[0]
		value := kvSplit[1]
		sorted[key] = append(sorted[key], value)
	}

}

func (w *Worker) runReduce(fnames []string, taskIndex int) error {
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
	// Run Reduce on list of values associated with each key and write output
	// to temporary file
	// TODO: Need to figure out how to write to file. Do we want to open a file
	// and repeatedly append to it (which is the pattern necessitated right now
	// by the way we have structured the Reduce function interface)? Or do we
	// want to write all the output at once? The latter would require an
	// adjustment to the Reduce function interface and the addition of some
	// intermediate data structure to store output data before write.
	for key, values := range collectedKVs {
		w.redFunc(key, values)
	}
	// Attempt atomic rename to final output file name
	return
}

// Run Worker execution flow
func (w *Worker) Run() {
	// Run heartbeat in the background as a goroutine
	return
}
