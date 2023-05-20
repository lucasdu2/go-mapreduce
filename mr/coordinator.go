package mr

import (
	"errors"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Set up supporting structs for coordinator
// =============================================================================
// workerInfo tracks the health of a worker and what task, if any, it is
// currently expected to be working on
type workerInfo struct {
	lastSeen     time.Time
	assignedTask *TaskInfo
}

// taskStack implements a concurrent stack type to manage task assignments
type taskStack struct {
	mu    sync.Mutex
	stack []*TaskInfo
}

// newMapTaskStack constructs a new taskStack from a list of Map task files
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

// newReduceTaskStack constructs a new taskStack from a list of Map task files
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
	taskCompLock      sync.Mutex   // Lock to protect access to taskCompletion
	taskCompletion    map[int]bool // Tracks task status (completed or not)
	taskAssigner      *taskStack   // Tracks idle tasks (to be assigned)
	intFilesLock      sync.Mutex   // Lock to protect intermediateFiles
	intermediateFiles [][]string   // Locations of intermediate files

	// Fields for stage transition coordination
	total     int        // Total tasks in a stage
	stageLock sync.Mutex // Lock to protect counter and stage string
	count     int        // Counter of completed tasks
	stage     string     // Current stage (Map, Reduce, or Finished)

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
	timeNow := time.Now()
	for i := 0; i < numWorkers; i++ {
		newWorker := workerInfo{timeNow, nil}
		coordinator.workers = append(coordinator.workers, newWorker)
	}
	// Construct taskCompletion map (initialize for Map stage)
	taskCompletion := make(map[int]bool)
	for i := 0; i < m; i++ {
		taskCompletion[i] = false
	}
	coordinator.taskCompletion = taskCompletion
	// Construct taskAssigner taskStack (initialize for Map stage)
	var taskFiles []string
	for i := 0; i < m; i++ {
		// NOTE: The coordinator code here expects Map task files to be of the
		// form pg-{index}.txt. The application-defined InputSplitter must
		// conform to this convention.
		taskFiles = append(taskFiles, "pg-"+strconv.Itoa(i)+".txt")
	}
	coordinator.taskAssigner = newMapTaskStack(taskFiles)
	// NOTE: For intermediateFiles, we pre-allocate an size r array. This allows
	// one index for every Reduce task, where each index stores the Map outputs
	// corresponding to that Reduce task. This automatically sorts intermediate
	// files by Reduce task and makes it easier to construct a taskStack for
	// the Reduce stage.
	coordinator.intermediateFiles = make([][]string, r)

	// Initialize total for map stage
	coordinator.total = m
	coordinator.count = 0
	coordinator.stage = "map"

	// Set up termination channel
	coordinator.killChan = kc

	return coordinator, nil
}

func (c *Coordinator) addToIntermediateFiles(outputFiles []string) {
	// NOTE: All intermediate files will be of the form workerN-X-Y, where X is
	// the index of the Map task, Y is the index of a Reduce task, and N is the
	// index of the worker that produced the file. Additionally, we expect all
	// intermediate files to be placed within a directory called workbench.

	// This will be the format that the workers must adhere to when creating
	// intermediate files and is the format assumed here.

	for _, filename := range outputFiles {
		// Get Reduce partition index from file name, given the name format
		// noted above
		splitOnDash := strings.Split(filename, "-")
		index, err := strconv.Atoi(splitOnDash[len(splitOnDash)-1])
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
	// If there was no previous task stage, this is the initial TaskRequest from
	// a worker and we should go straight to task assignment
	if args.PrevTaskStage == "" {
		log.Printf("Initial task assignment for Worker %v\n", args.WorkerIndex)
		return
	}

	// If the completed task is for a previous stage, reject the completion
	if args.PrevTaskStage != c.checkStage() {
		log.Println(args.PrevTaskStage)
		log.Println(c.checkStage())
		log.Printf("Completed task from previous stage, ignoring")
		return
	}
	log.Printf("Worker %v has completed Task %v from Stage %v",
		args.WorkerIndex, args.PrevTaskIndex, args.PrevTaskStage)

	// NOTE: We need to acquire a lock when checking completion status in
	// taskCompletion. Multiple workers can concurrently report that they have
	// completed the same task, so there can be concurrent executions of
	// handleTaskCompletion. We must avoid the situation where multiple workers
	// see the task in not yet completed and enter the completion flow, since
	// this would result in double counting of a task in the task counter.
	c.taskCompLock.Lock()
	defer c.taskCompLock.Unlock()
	// If task is not already completed, run task completion flow
	if !c.taskCompletion[args.PrevTaskIndex] {
		log.Printf("Handling task completion")
		if c.checkStage() == "map" {
			c.addToIntermediateFiles(args.OutputFiles)
		}
		// Set task status to completed
		c.taskCompletion[args.PrevTaskIndex] = true
		// Increment completed tasks counter
		log.Printf("Incrementing task counter")
		c.countInc()
	} else {
		log.Printf("Task already completed")
	}
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskInfo) error {
	log.Printf("Handling AssignTask request from Worker %v\n", args.WorkerIndex)
	// Handle logic when a worker has completed a task
	c.handleTaskCompletion(args)
	// Assign new task to worker, if possible
	var err error
	nextTask, err := c.taskAssigner.pop()
	log.Printf("Getting next task from task assignment queue")
	// pop() will only return an non-nil error is there are no more tasks to
	// assign. If this is the case, wait until all other tasks in stage are done
	// before continuing.
	if err != nil {
		log.Print("No more tasks in task queue, waiting...")
		// Update worker in workers (workerInfo slice) with a "no task" indicator
		c.workers[args.WorkerIndex].assignedTask = nil
		// To synchronize stage completion while still allowing waiting threads
		// to take requeued tasks, we spin on c.taskAssigner.pop() with a
		// random wait between attempts.
		currentStage := c.checkStage()
		if currentStage == "finished" {
			reply.Stage = "finished"
			return nil
		}
		for currentStage == c.checkStage() {
			nextTask, err = c.taskAssigner.pop()
			// If stage is not over, but there are no outstanding tasks in the
			// queue, we wait a random time and retry
			if err != nil {
				waitDuration := time.Duration(rand.Intn(250))
				time.Sleep(waitDuration * time.Millisecond)
			}
		}
	}
	if c.checkStage() == "finished" {
		reply.Stage = "finished"
		return nil
	}
	// There should always be a new task popped from taskAssigner and assigned
	// to nextTask at this point--if that has not yet happened, we do the
	// assignment here
	if nextTask == nil {
		nextTask, err = c.taskAssigner.pop()
		if err != nil {
			// If taskAssigner is still empty, the MapReduce operation must be
			// over and we should return accordingly
			reply.Stage = "finished"
			return nil
		}
	}
	// Deep copy nextTask into reply struct
	reply.TaskIndex = nextTask.TaskIndex
	reply.FilesLocation = nextTask.FilesLocation
	reply.Stage = nextTask.Stage
	log.Printf("Assigned Task %v from Stage %v to Worker %v\n", reply.TaskIndex,
		reply.Stage, args.WorkerIndex)
	// Update worker status (in workers []workerInfo) with newly assigned task
	c.workers[args.WorkerIndex].assignedTask = nextTask
	return nil
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
		log.Printf("Stage is over, reached expected number of tasks: %v", c.total)
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
	c.taskCompletion = taskCompletion

}

// Set up Coordinator for Reduce stage
func (c *Coordinator) setupReduce() {
	// Reset expected total to R (number of Reduce tasks)
	c.total = c.r

	// Set up taskCompletion map for Reduce stage
	c.createReduceTaskCompletionMap()

	// Fill out taskAssigner taskStack (initialize for Map stage)
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

func (c *Coordinator) WorkerHeartbeat(args *int, reply *bool) error {
	// Update lastSeen field in workerInfo for worker sending heartbeat and
	// send an acknowledgement back to worker
	if args != nil {
		t := time.Now()
		c.workers[*args].lastSeen = t
		log.Printf("Received hearbeat from Worker %v at %v", *args, t)
		*reply = true
	}
	return nil
}

func (c *Coordinator) monitorWorkerHealth(workerIndex int) {
	log.Printf("Started monitoring health of Worker %v", workerIndex)
	// Continuously check if worker is still considered to be alive
	for true {
		// Sleep for 1 second between checks--workers should send a hearbeat
		// every 100ms, so waiting for 1 second between checks should guarantee
		// a change in lastSeen if worker is sending regular heartbeats
		sleepInterval := time.Second
		time.Sleep(sleepInterval)
		status := c.workers[workerIndex]
		log.Printf("Worker %v last seen: %v", workerIndex, status.lastSeen)
		// Consider worker to be dead if current time is more than 1 second
		// after the worker's last seen time
		if time.Now().After(status.lastSeen.Add(time.Second)) {
			log.Printf("Coordinator considers Worker %v dead", workerIndex)
			// If worker considered dead, move current assigned task back into
			// task queue if task is not already completed
			c.taskCompLock.Lock()
			defer c.taskCompLock.Unlock()

			// If task is not already completed by another worker, add the task
			// back to task stack
			crashedTask := status.assignedTask
			if !c.taskCompletion[crashedTask.TaskIndex] {
				c.taskAssigner.push(crashedTask)
				log.Printf("Task %v from Stage %v re-added to task stack "+
					"(previously assigned to Worker %v)", crashedTask.TaskIndex,
					crashedTask.Stage, workerIndex)
			}
		}
	}
}
