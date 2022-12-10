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
	cond  sync.Cond
	total int
	count int
	done  bool // Indicates when stage is over
}

func newTaskCounter(totalTasks int) *taskCounter {
	tc := &taskCounter{sync.NewCond(sync.Mutex{}), totalTasks, 0, false}
	return tc
}

func (tc *taskCounter) inc() bool {
	tc.cond.L.Lock()
	defer tc.cond.L.Unlock()
	tc.count++
	if tc.count == tc.totalTasks {
		// Once we finish all tasks in a stage, broadcast to all waiting RPC
		// handlers that stage is over
		tc.done = true
		tc.cond.Broadcast()
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
	taskCounter       *taskCounter     // Counter of total completed tasks
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
	coordinator.taskCounter = newTaskCounter()
	// Initialize intermediateFiles map
	coordinator.intermediateFiles = make(map[int][]string)
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskInfo) error {
	if args.prevTaskCompleted {
		// Check if task has already been completed
		// TODO
		// Increment completed tasks counter
		if c.numTasksCompleted.inc() {
			// If the last task is now completed, handle end of stage logic
		}
	}
	var err error
	reply, err = c.taskAssigner.pop()
	if err != nil {
		for !c.taskCounter.done {
			c.taskCounter.cond.Wait()
		}
	}
	// TODO: If no more items to assign, wait until all tasks in the stage are
	// completed then return
}

func (c *Coordinator) CheckWorker() {

}

// Set up Coordinator for Reduce stage
func (c *Coordinator) setupReduce() {

}

// Run Coordinator execution flow
func (c *Coordinator) Run() {

}
