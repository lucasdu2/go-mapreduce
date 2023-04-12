package mr

type TaskRequest struct {
	WorkerIndex       int       // Index of worker
	PrevTaskIndex     int       // Index of previous task
	PrevTaskStage     string    // Stage that previous task belonged to
	PrevTaskCompleted bool      // Indicate if previously assigned task completed
	OutputFiles       []string  // Return completed task output files
	PrevTaskInfo      *TaskInfo // Previous TaskInfo struct
}

// TaskInfo defines a struct to pass information about a Map or Reduce task to
// a worker. We define filesLocation as a slice of string slices in order to
// handle both a single input file (Map task) and a list of input files (Reduce
// task).
type TaskInfo struct {
	TaskIndex     int
	FilesLocation []string
	Stage         string
}
