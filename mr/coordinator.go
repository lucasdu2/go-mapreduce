package mapreduce

import (
	"errors"
	"strconv"
	"sync"
)

// Set up supporting structs for coordinator
// =============================================================================
type workerInfo struct {
	up        bool
	taskIndex int
}

type TaskInfo struct {
	taskIndex    int
	fileLocation string
	stage        string
}

// Implement concurrent stack type to manage task assignments
type taskStack struct {
	mu    sync.Mutex
	stack []TaskInfo
}

func newTaskStack(taskFiles []string, stage string) *taskStack {
	// Create taskStack struct and populate
	ts := &taskStack{sync.Mutex{}, make([]TaskInfo, 0)}
	for i, fname := range taskFiles {
		// Create TaskInfo struct for each task file
		ti := &TaskInfo{
			taskIndex:    i,
			fileLocation: fname,
			stage:        stage,
		}
		append(ts.stack, ti)
	}
	return ts
}

func (s *taskStack) push(t TaskInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	append(s.stack, t)
}

func (s *taskStack) pop() (TaskInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := len(s.stack)
	if l == 0 {
		return nil, errors.New("taskStack is empty")
	}
	ret := s.stack[l-1]
	s.stack = s.stack[:l-1]
	return ret, nil
}

// Implement atomic counter to track number of completed tasks
type taskCounter struct {
	mu    sync.Mutex
	total int
	count int
}

func newTaskCounter(totalTasks int) *taskCounter {
	tc := &taskCounter{sync.Mutex{}, totalTasks, 0}
	return tc
}

func (c *taskCounter) inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	if c.count == c.totalTasks {
		// TODO: Do something here to indicate that all expected tasks have
		// completed and the stage is over
	}
}

// Implement coordinator
// =============================================================================
// Define core Coordinator struct
type Coordinator struct {
	M                 int              // Number of Map tasks
	R                 int              // Number of Reduce tasks
	workers           []workerInfo     // Tracks worker health, assigned task
	taskCompletion    map[int]bool     // Tracks task status (completed or not)
	numTasksCompleted *taskCounter     // Counter of total completed tasks
	taskAssigner      *taskStack       // Tracks idle tasks (to be assigned)
	intermediateFiles map[int][]string // Locations of intermediate files
}

// Create a new Coordinator
func NewCoordinator(m, r, numWorkers int) (*Coordinator, error) {
	coordinator := &Coordinator{}
	// Fill in Coordinator fields
	coordinator.M = m
	coordinator.R = r
	// Construct workers slice
	workers := make([]workerInfo, numWorkers)
	for i := 0; i < numWorkers; i++ {
		newWorker := &workerInfo{true, i}
		append(workers, newWorker)
	}
	coordinator.workers = workers
	// Construct taskCompletion map (initialize for Map stage)
	taskCompletion := make(map[int]bool)
	for i := 0; i < m; i++ {
		taskCompletion[i] = false
	}
	coordinator.taskCompletion = taskCompletion
	// Construct taskAssigner taskStack (initialize for Map stage)
	taskFiles := make([]string, m)
	for i := 0; i < m; i++ {
		// NOTE: The coordinator code here expects Map task files to be of the
		// form pg-{index}.txt. The application-defined InputSplitter must
		// conform to this convention.
		append(taskFiles, "pg-"+strconv.Itoa(i)+".txt")
	}
	coordinator.taskAssigner = newTaskStack(taskFiles, "map")
	// Initialize numTasksCompleted taskCounter
	coordinator.numTasksCompleted = newTaskCounter()
	// Initialize intermediateFiles map
	coordinator.intermediateFiles = make(map[int][]string)
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskInfo) error {
	if args.prevTaskCompleted {
		c.numTasksCompleted.inc()
	}
	var err error
	reply, err = c.taskAssigner.pop()
	// TODO: Handle error case

	// NOTES: Task assignment, stage completion logic:
	// - If no tasks waiting to be assigned, implement Backup Tasks (as
	//	 described in the MapReduce paper). Look for in-progress tasks (need
	//	 efficient way to do this) and assign to the idle worker.
	// - Once all tasks are done, need way to signal to all workers to stop work,
	//	 need way to signal end of the stage
	//		- Can probably just do this by having another RPC handler that the
	//		  workers will call in a loop (with some time between each call),
	//		  that will check to see if the all tasks are done/the stage is
	//		  complete. Maybe this can piggyback off of the heartbeat RPC to
	//		  make it more network efficient. Additionally, to save time,
	//		  idle workers (workers that have finished all assigned tasks and
	//		  are not working on a Backup Task) can immediately request a task
	//		  from the new stage, minimizing the latency that can be caused by
	//		  time-interval based RPC checks from the worker. For this case,
	//		  we can employ sync.Cond Wait (see below) for immediate notification
	//		  upon full task completion.
	// - ASIDE: If we don't implement Backup Tasks, we can simply have a sync.Cond
	//	 Wait in this RPC handler that waits for the number of completed tasks
	//	 to reach the expected total.
}

func (c *Coordinator) CheckWorker() {

}

// Set up Coordinator for Reduce stage
func (c *Coordinator) setupReduce() {

}

// Run Coordinator execution flow
func (c *Coordinator) Run() {

}
