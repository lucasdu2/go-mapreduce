package mapreduce

import (
	"errors"
	"strconv"
	"strings"
	"sync"
)

// Set up supporting structs for coordinator
// =============================================================================
// workerInfo tracks the health of a worker and what task, if any, it is
// currently expected to be working on
type workerInfo struct {
	up        bool
	taskIndex int
}

// TaskInfo defines a struct to pass information about a Map or Reduce task to
// a worker. We define filesLocation as a slice of string slices in order to
// handle both a single input file (Map task) and a list of input files (Reduce
// task).
type TaskInfo struct {
	taskIndex     int
	filesLocation [][]string
	stage         string
}

// taskStack implements a concurrent stack type to manage task assignments
type taskStack struct {
	mu    sync.Mutex
	stack []*TaskInfo
}

// newMapTaskStack constructs a
func newMapTaskStack(taskFiles []string) *taskStack {
	// Create taskStack struct and populate
	ts := &taskStack{sync.Mutex{}, make([]TaskInfo, 0)}
	for i, fname := range taskFiles {
		// Create TaskInfo struct for each task file
		ti := &TaskInfo{
			taskIndex:     i,
			filesLocation: []string{fname},
			stage:         "map",
		}
		append(ts.stack, ti)
	}
	return ts
}

func newReduceTaskStack(taskFiles [][]string) *taskStack {
	// Create taskStack struct and populate
	ts := &taskStack{sync.Mutex{}, make([]TaskInfo, 0)}
	for i, fname := range taskFiles {
		// Create TaskInfo struct for each task file
		ti := &TaskInfo{
			taskIndex:    i,
			fileLocation: fname,
			stage:        "reduce",
		}
		append(ts.stack, ti)
	}
	return ts
}

func (s *taskStack) push(t *TaskInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	append(s.stack, t)
}

func (s *taskStack) pop() (*TaskInfo, error) {
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

// Implement coordinator
// =============================================================================
// Define core Coordinator struct
type Coordinator struct {
	// NOTE: Both workers and intermediateFiles slices can be concurrently
	// accessed. Multiple threads can access the same index in intermediateFiles,
	// so we must protect it with a lock. For workers, it is only modified in
	// AssignTask, and each index will only be modified by one worker. Due to
	// the fact that if the coordinator is down, the MapReduce operation is
	// considered terminated, a worker should never submit overlapping requests
	// for a new task. So each index in workers will be modified sequentially,
	// thus precluding the need for a lock.
	M                 int          // Number of Map tasks
	R                 int          // Number of Reduce tasks
	workers           []workerInfo // Tracks worker health, assigned task
	taskCompletion    map[int]bool // Tracks task status (completed or not)
	taskAssigner      *taskStack   // Tracks idle tasks (to be assigned)
	intFilesLock      sync.Mutex   // Lock to protect intermediateFiles
	intermediateFiles [][]string   // Locations of intermediate files
	stage             string       // Declare current stage (Map or Reduce)

	// Fields for atomic task counter
	cond  sync.Cond // Used to synchronize stage completion
	total int       // Total tasks in a stage
	count int       // Counter of completed tasks
	done  bool      // Indicates when stage is over
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
	coordinator.taskAssigner = newMapTaskStack(taskFiles)
	// Initialize intermediateFiles
	// NOTE: We initialize an r-size array on string slices. This gives one
	// index for every Reduce task, which allows us to easily sort intermediate
	// files by Reduce task and construct a taskStack for the Reduce stage.
	coordinator.intermediateFiles = [r][]string{}
	coordinator.stage = "map"

	// Initialize fields for atomic task counter
	coordinator.cond = sync.NewCond(sync.Mutex{})
	// Initialize total for map stage
	coordinator.total = m
	coordinator.count = 0
	coordinator.done = false

}

func (c *Coordinator) addToIntermediateFiles(mapTaskIndex int) {
	// Acquire lock before updating intermediateFiles slice
	// See NOTE in Coordinator struct for reasoning
	c.intFilesLock.Lock()
	defer c.intFilesLock.Unlock()
	// NOTE: All intermediate files will be of the form mr-X-Y, where X is the
	// index of the Map task and Y is the index of the Reduce task. This will
	// be the format that the workers must adhere to when creating intermediate
	// files and is the format assumed here.
	var sb strings.Builder
	for i := 0; i < c.R; i++ {
		sb.Reset()
		sb.WriteString("mr-")
		sb.WriteString(strconv.Itoa(mapTaskIndex))
		sb.WriteString(strconv.Itoa(i))
		filename := sb.String()
		append(c.intermediateFiles[i], filename)
	}
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskInfo) error {
	// Handle logic when a worker has completed a task
	if args.prevTaskCompleted {
		// Only add to intermediate files (for Map tasks) and increment
		// completed task counter if task has not already been completed
		if !c.taskCompletion[args.prevTaskIndex] {
			if c.stage == "map" {
				addToIntermediateFiles(args.prevTaskIndex)
			}
			// Increment completed tasks counter
			c.countInc()
		}
	}
	// Assign new task to worker, if possible
	var err error
	reply, err = c.taskAssigner.pop()
	// pop() will only return an non-nil error is there are no more tasks to
	// assign. If this is the case, wait until all other tasks in stage are done
	// before continuing.
	if err != nil {
		// Update worker in workers (workerInfo slice) with a "no task" indicator
		c.workers[args.workerIndex].taskIndex = -1
		// NOTE: For why we should wrap the Wait() in a for loop spinning on
		// done, see here: https://stackoverflow.com/questions/33841585
		for !c.done {
			c.cond.Wait()
		}
		// If the next stage is Reduce, the task stack will have been refilled
		// at this point and we will be able to pop from it. If the stack is
		// still empty, the Reduce stage is complete, theMapReduce operation is
		// over, and we should return.
		reply, err = c.taskAssigner.pop()
		if err != nil {
			// MapReduce operation is over
			// TODO: Should probably send a message in reply to tell worker to
			// shut down
			// TODO: Should also clean up all intermediate files that we used
			return nil
		}
	}
	// Update worker status (in workers []workerInfo) with newly assigned task
	c.workers[args.workerIndex].taskIndex = reply.taskIndex
	return nil
}

func (c *Coordinator) CheckWorker() {
	// TODO: Is it possible for a task to be completed but still in the
	// taskAssigner stack? For example, if we have 2 workers working on the
	// same task, but one fails while the other succeeds. We need to work
	// something into worker failed logic (in CheckWorker) that prevents
	// an already completed task from being requeued.
}

// countInc implements an atomic increment for the coordinator's completed
// tasks counter
func (c *Coordinator) countInc() {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	c.count++
	// Handle logic when all tasks in stage are completed
	if c.count == c.totalTasks {
		// Set up for Reduce stage or end MapReduce operation
		if c.stage == "map" {
			c.stage = "reduce"
			c.setupReduce()
		}
		// Set done to true
		c.done = true
		// Broadcast to all waiting RPC handlers that stage is over
		c.cond.Broadcast()
	}
}

// Set up Coordinator for Reduce stage
func (c *Coordinator) setupReduce() {
	// Construct taskCompletion map (initialize for Reduce stage)
	taskCompletion := make(map[int]bool)
	for i := 0; i < c.R; i++ {
		taskCompletion[i] = false
	}
	// Construct taskAssigner taskStack (initialize for Map stage)
	coordinator.taskAssigner = newReduceTaskStack(c.intermediateFiles)
	c.stage = "reduce"
	c.total = c.R
	c.count = 0
	c.done = false
}

// Run Coordinator execution flow
func (c *Coordinator) Run() {

}
