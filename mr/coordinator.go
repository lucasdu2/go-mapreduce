package mr

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

// Set up supporting structs for coordinator
// =============================================================================
// workerInfo tracks the health of a worker and what task, if any, it is
// currently expected to be working on
type workerInfo struct {
	up        bool
	taskIndex int
}

// taskStack implements a concurrent stack type to manage task assignments
type taskStack struct {
	mu    sync.Mutex
	stack []*TaskInfo
}

// newMapTaskStack constructs a
func newMapTaskStack(taskFiles []string) *taskStack {
	// Create taskStack struct and populate
	ts := &taskStack{sync.Mutex{}, make([]*TaskInfo, 0)}
	for i, fname := range taskFiles {
		// Create TaskInfo struct for each task file
		ti := &TaskInfo{
			TaskIndex:     i,
			FilesLocation: []string{fname},
			Stage:         "map",
		}

		ts.stack = append(ts.stack, ti)
	}
	return ts
}

func newReduceTaskStack(taskFiles [][]string) *taskStack {
	// Create taskStack struct and populate
	ts := &taskStack{sync.Mutex{}, make([]*TaskInfo, 0)}
	for i, fnames := range taskFiles {
		// Create TaskInfo struct for each task file
		ti := &TaskInfo{
			TaskIndex:     i,
			FilesLocation: fnames,
			Stage:         "reduce",
		}
		ts.stack = append(ts.stack, ti)
	}
	return ts
}

func (s *taskStack) push(t *TaskInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stack = append(s.stack, t)
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
	m                 int          // Number of Map tasks
	r                 int          // Number of Reduce tasks
	workers           []workerInfo // Tracks worker health, assigned task
	taskCompLock      *sync.Mutex  // Lock to protect access to taskCompletion
	taskCompletion    map[int]bool // Tracks task status (completed or not)
	taskAssigner      *taskStack   // Tracks idle tasks (to be assigned)
	intFilesLock      *sync.Mutex  // Lock to protect intermediateFiles
	intermediateFiles [][]string   // Locations of intermediate files

	// Fields for stage transition coordination
	total     int         // Total tasks in a stage
	stageLock *sync.Mutex // Lock to protect counter and stage string
	count     int         // Counter of completed tasks
	stage     string      // Current stage (Map, Reduce, or Finished)

	// Channel to communicate termination
	killChan chan int
}

// Create a new Coordinator
func newCoordinator(m, r, numWorkers int, kc chan int) (*Coordinator, error) {
	coordinator := &Coordinator{}
	// Fill in Coordinator fields
	coordinator.m = m
	coordinator.r = r
	// Construct workers slice
	workers := make([]workerInfo, numWorkers)
	for i := 0; i < numWorkers; i++ {
		newWorker := workerInfo{true, i}
		workers = append(workers, newWorker)
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
		taskFiles = append(taskFiles, "pg-"+strconv.Itoa(i)+".txt")
	}
	coordinator.taskAssigner = newMapTaskStack(taskFiles)
	// NOTE: For intermediateFiles, we pre-allocate an r x m array. This allows
	// one index for every Reduce task, where each index stores the m Map outputs
	// corresponding to that Reduce task. This automatically sorts intermediate
	// files by Reduce task and makes it easier to construct a taskStack for
	// the Reduce stage.
	coordinator.intermediateFiles = make([][]string, r)
	for i := 0; i < r; i++ {
		coordinator.intermediateFiles[i] = make([]string, m)
	}

	// Initialize total for map stage
	coordinator.total = m
	coordinator.count = 0
	coordinator.stage = "map"

	// Set up termination channel
	coordinator.killChan = kc

	return coordinator, nil
}

func (c *Coordinator) addToIntermediateFiles(OutputFiles []string) {
	// Acquire lock before updating intermediateFiles slice
	// See NOTE in Coordinator struct for reasoning
	c.intFilesLock.Lock()
	defer c.intFilesLock.Unlock()

	// NOTE: All intermediate files will be of the form workerN-X-Y, where X is
	// the index of the Map task, Y is the index of a Reduce task, and N is the
	// index of the worker that produced the file. Additionally, we expect all
	// intermediate files to be placed within a directory called workbench.

	// This will be the format that the workers must adhere to when creating
	// intermediate files and is the format assumed here.

	for _, filename := range OutputFiles {
		// Get Reduce partition index fron file name, given the name format
		// noted above
		index, err := strconv.Atoi(filename[len(filename)-1:])
		if err != nil {
			log.Panic("Unable to get Reduce partition index from file name")
		}
		c.intermediateFiles[index] = append(c.intermediateFiles[index], filename)
	}
}

func (c *Coordinator) checkStage() string {
	c.stageLock.Lock()
	defer c.stageLock.Unlock()
	return c.stage
}

func (c *Coordinator) handleTaskCompletion(args *TaskRequest) {
	c.taskCompLock.Lock()
	defer c.taskCompLock.Unlock()

	// If the completed task is for a previous stage, reject the completion
	if args.PrevTaskStage != c.checkStage() {
		return
	}

	// If task is not already completed, run task completion flow
	if !c.taskCompletion[args.PrevTaskIndex] {
		if c.stage == "map" {
			c.addToIntermediateFiles(args.OutputFiles)
		}
		// Set task status to completed
		c.taskCompletion[args.PrevTaskIndex] = true
		// Increment completed tasks counter
		c.countInc()
	}

}

func (c *Coordinator) handleTaskFailure(args *TaskRequest) {
	c.taskCompLock.Lock()
	defer c.taskCompLock.Unlock()

	// If the failed task is for a previous stage, do nothing
	if args.PrevTaskStage != c.checkStage() {
		return
	}

	// If task is not already completed by another worker, requeue the task
	if !c.taskCompletion[args.PrevTaskIndex] {
		// If the previous task failed, the prevTaskInfo filed of TaskRequest
		// should be filled out with the previous TaskInfo struct
		c.taskAssigner.push(args.PrevTaskInfo)
	}

}
func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskInfo) error {
	// Handle logic when a worker has completed a task
	if args.PrevTaskCompleted {
		// Only add to intermediate files (for Map tasks) and increment
		// completed task counter if task has not already been completed
		// NOTE: Multiple workers can concurrently report that they have
		// completed the same task, so there can be concurrent executions of
		// this section of code. In particular, there can be concurrent access
		// to taskCompletion, where multiple workers may see that the task has
		// not yet been completed and enter the if statement.
		// Within the if statement, addToIntermediateFiles already synchronizes
		// writes to intermediateFiles, and setting task status to completed
		// is an idempotent action. However, we run the risk of incrementing the
		// completed tasks counter multiple times if multiple workers complete
		// the same task and enter this section concurrently. This behavior is
		// incorrect and so we must still synchronize this entire section of
		// code with a lock.
		c.handleTaskCompletion(args)
	} else {
		// Handle logic when a worker was not able to complete a task
		c.handleTaskFailure(args)
	}
	// Assign new task to worker, if possible
	var err error
	reply, err = c.taskAssigner.pop()
	// pop() will only return an non-nil error is there are no more tasks to
	// assign. If this is the case, wait until all other tasks in stage are done
	// before continuing.
	if err != nil {
		// Update worker in workers (workerInfo slice) with a "no task" indicator
		c.workers[args.WorkerIndex].taskIndex = -1
		// To synchronize stage completion while still allowing waiting threads
		// to take requeued tasks, we spin on c.taskAssigner.pop() with a
		// random wait between attempts.
		currentStage := c.checkStage()
		if currentStage == "finished" {
			reply = &TaskInfo{Stage: "finished"}
			return nil
		}
		for currentStage == c.checkStage() {
			reply, err = c.taskAssigner.pop()
			// If stage is not over, but there are no outstanding tasks in the
			// queue, we wait a random time and retry
			if err != nil {
				waitDuration := time.Duration(rand.Intn(250))
				time.Sleep(waitDuration * time.Millisecond)
			}
		}
	}
	if c.checkStage() == "finished" {
		reply = &TaskInfo{Stage: "finished"}
		return nil
	}
	// Update worker status (in workers []workerInfo) with newly assigned task
	c.workers[args.WorkerIndex].taskIndex = reply.TaskIndex
	return nil
}

func (c *Coordinator) CheckWorker() {
	// IDEA: Should add another field to workerInfo that tracks last seen
	// timestamp. This field should be updated by this CheckWorker function.
	// Then have some function that periodically checks this field and how much
	// time has passed since the worker was last seen. If a certain amount of
	// time has elapsed since the last seen time, we should set the worker to
	// unhealthy and re-add its task the task queue.
	// TODO: Is it possible for a task to be completed but still in the
	// taskAssigner stack? For example, if we have 2 workers working on the
	// same task, but one fails while the other succeeds. We need to work
	// something into worker failed logic (in CheckWorker) that prevents
	// an already completed task from being requeued.

}

// countInc implements an atomic increment for the coordinator's completed
// tasks counter. Once the task counter hits the total expected completed tasks
// for a stage, countInc will also update the current stage string in the same
// atomic operation.
func (c *Coordinator) countInc() {
	c.stageLock.Lock()
	defer c.stageLock.Unlock()
	c.count++
	// Handle logic when all tasks in stage are completed
	if c.count == c.total {
		// Set up for Reduce stage or end MapReduce operation
		if c.stage == "map" {
			c.stage = "reduce"
			c.count = 0
			c.setupReduce()
		} else if c.stage == "reduce" {
			c.stage = "finished"
			// Also, send termination message on killChan to start graceful
			// shutdown of the server
			c.killChan <- 1
		}
	}
}

func (c *Coordinator) createReduceTaskCompletionMap() {
	// Construct taskCompletion map (initialize for Reduce stage)
	taskCompletion := make(map[int]bool)
	for i := 0; i < c.r; i++ {
		taskCompletion[i] = false
	}

	c.taskCompLock.Lock()
	defer c.taskCompLock.Unlock()

	c.taskCompletion = taskCompletion

}

// Set up Coordinator for Reduce stage
func (c *Coordinator) setupReduce() {
	// Reset expected total to R (number of Reduce tasks)
	c.total = c.r

	// Set up taskCompletion map for Reduce stage
	c.createReduceTaskCompletionMap()

	// Fill out taskAssigner taskStack (initialize for Map stage)
	c.taskAssigner.mu.Lock()
	defer c.taskAssigner.mu.Unlock()
	for i, fnames := range c.intermediateFiles {
		// Create TaskInfo struct for each task file
		ti := &TaskInfo{
			TaskIndex:     i,
			FilesLocation: fnames,
			Stage:         "reduce",
		}
		c.taskAssigner.stack = append(c.taskAssigner.stack, ti)
	}
}

// Run Coordinator execution flow
func CoordinatorRun(m, r, numWorkers int, kc chan int) {
	// Initialize Coordinator struct
	c, err := newCoordinator(m, r, numWorkers, kc)
	if err != nil {
		log.Fatal("error creating new coordinator: ", err)
	}
	// Start RPC server
	rpc.Register(c)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	// TODO: Figure out why we start a new server here, or if we need to use the
	// already-started RPC server rpc.DefaultServer...
	// ANSWER: Need to start a new HTTP server here, see this link (take notes
	// on this later): medium.com/rungo/building-rpc-remote-procedure-call-
	// network-in-go-5bfebe90f7e9. Basically, we need an RPC server (defined
	// above) and an HTTP server to host the RPC server (defined below).
	srv := &http.Server{}
	go func() {
		err := srv.Serve(l)
		if err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
		log.Println("Stopped serving new connections.")
	}()
	// Block on killChan
	<-c.killChan
	// Shutdown server gracefully when termination message is received on
	// killChan
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatal("HTTP shutdown error: ", err)
	}
}
