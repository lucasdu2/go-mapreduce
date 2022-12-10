package mapreduce

import "os"

type TaskRequest struct {
	prevTaskIndex     int    // Index of previous task
	prevTaskCompleted bool   // Indicate if previously assigned task completed
	prevTaskFile      string // Return completed task output file
	// TODO
}

type Worker struct {
	mapFunc func(string, *map[string]string) error
	redFunc func(string, []string, *os.File) error
}
