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
	stack []*TaskInfo
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
	M                 int              // Number of Map tasks
	R                 int              // Number of Reduce tasks
	workers           []workerInfo     // Tracks worker health, assigned task
	taskCompletion    map[int]bool     // Tracks task status (completed or not)
	taskAssigner      *taskStack       // Tracks idle tasks (to be assigned)
	intermediateFiles map[int][]string // Locations of intermediate files
	stage             string           // Declare current stage (Map or Reduce)

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
	coordinator.taskAssigner = newTaskStack(taskFiles, "map")
	// Initialize intermediateFiles map
	coordinator.intermediateFiles = make(map[int][]string)
	coordinator.stage = "map"

	// Initialize fields for atomic task counter
	coordinator.cond = sync.NewCond(sync.Mutex{})
	// Initialize total for map stage
	coordinator.total = m
	coordinator.count = 0
	coordinator.done = false

}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskInfo) error {
	// Handle logic when a worker has completed a task
	if args.prevTaskCompleted {
		// Check if task has already been completed
		if !c.taskCompletion[args.prevTaskIndex] {
			// TODO: add to intermediate files
			// Increment completed tasks counter
			c.countInc()
		}
	}
	// Assign new task to worker, if possible
	var err error
	reply, err = c.taskAssigner.pop()
	// pop() will only return an non-nil error is there are no more tasks to
	// assing. If this is the case, wait until all other tasks in stage are done
	// before continuing.
	if err != nil {
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
			// TODO: SHould also clean up all intermediate files that we used
			return nil
		}
	}
	return nil
}

func (c *Coordinator) CheckWorker() {

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
	taskFiles := make([]string, c.R)
	for i := 0; i < c.R; i++ {
		// NOTE: The coordinator code here expects Map task files to be of the
		// form pg-{index}.txt. The application-defined InputSplitter must
		// conform to this convention.
		// TODO: Below is NOT correct, need to fix
		append(taskFiles, c.intermediateFiles[i])
	}
	coordinator.taskAssigner = newTaskStack(taskFiles, "reduce")

	c.stage = "reduce"
	c.total = c.R
	c.count = 0
	c.done = false
}

// Run Coordinator execution flow
func (c *Coordinator) Run() {

}
