package mapreduce

import "os"

type TaskRequest struct {
	prevTaskCompleted bool   // Indicate if previously assigned task completed
	prevTaskFile      string // Return completed task output file
	// TODO
}

type Worker struct {
	mapFunc func(string, *map[string]string) error
	redFunc func(string, []string, *os.File) error
}
