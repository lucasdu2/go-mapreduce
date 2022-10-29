package mapreduce

import (
	"plugin"
)

type WorkerInfo struct {
	up        bool
	taskIndex int
}

type TaskInfo struct {
	taskIndex    int
	fileLocation string
	mrFunction   string
}

// Implement concurrent stack for idleTasks
//type taskStack []TaskInfo
//
//func (s *taskStack) initStack(taskFiles []str) {
//	for
//}
//
//func (s *taskStack) push() {
//
//}
//
//func (s *taskStack) pop() {
//
//}

// NOTE: Need to think about how to leverage channels in this program (instead
// of just using mutexes).

type Coordinator struct {
	mapFunc           plugin.Symbol
	redFunc           plugin.Symbol
	M                 int
	R                 int
	workers           []WorkerInfo
	idleTasks         []TaskInfo
	compTasksCount    int
	intermediateFiles []string
}

// Create a new Coordinator, set up for Map stage
func (c *Coordinator) newCoordinator() (Coordinator, error) {

}

// Set up Coordinator for Reduce stage
func (c *Coordinator) setupReduce() {

}

func (c *Coordinator) AssignTask() {

}

func (c *Coordinator) CheckWorker() {

}

// Coordinator execution flow
func runCoordinator() {

}
