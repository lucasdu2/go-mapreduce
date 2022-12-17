package mapreduce

import "os"

type TaskRequest struct {
	workerIndex       int    // Index of worker
	prevTaskIndex     int    // Index of previous task
	prevTaskCompleted bool   // Indicate if previously assigned task completed
	prevTaskFile      string // Return completed task output file
}

type Worker struct {
	index   int
	mapFunc func(string, *map[string]string) error
	redFunc func(string, []string, *os.File) error
}

func (w *Worker) runMap() {
	// Read data from file
	// Run Map function on data
	// Sort keys into R partitions using partitioning function
	// Write resulting data to intermediate files
	return
}

func (w *Worker) runReduce() {
	// Read data from each file
	// Sort by intermediate keys
	// Run Reduce on each key
	// Write output to temporary file
	// Attempt atomic rename to final output file name
	return
}

// Run Worker execution flow
func (w *Worker) Run() {
	return
}
