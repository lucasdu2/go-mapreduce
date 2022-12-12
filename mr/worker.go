package mapreduce

import "os"

type TaskRequest struct {
	workerIndex       int    // Index of worker
	prevTaskIndex     int    // Index of previous task
	prevTaskCompleted bool   // Indicate if previously assigned task completed
	prevTaskFile      string // Return completed task output file
	// TODO
}

type Worker struct {
	index   int
	mapFunc func(string, *map[string]string) error
	redFunc func(string, []string, *os.File) error
}

// Run Worker execution flow
func (w *Worker) Run() {

}
