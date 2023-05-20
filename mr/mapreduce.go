package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
	"time"
)

func RunMapReduce(
	m, r, numWorkers int,
	mapFunc, redFunc, partFunc plugin.Symbol,
	crashCount int,
	kc chan int,
) {

	// Spawn Coordinator and Worker programs
	// NOTE: Because we do not have access to an actual cluster of servers, we
	// will simply be starting the Coordinator and Workers on a single local
	// machine as concurrent goroutines that communicate using RPCs.

	// First, set up and start Coordinator
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
	// NOTE: We need to start a new HTTP server here. Even though we have
	// already set up an RPC server (defined above), we still need an HTTP
	// server to host the RPC server.
	srv := &http.Server{}
	go func() {
		err := srv.Serve(l)
		if err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
		log.Println("Stopped serving new connections.")
	}()

	// Then start all Workers in goroutines
	for i := 0; i < numWorkers; i++ {
		// Start monitoring worker health
		go c.monitorWorkerHealth(i)
		// Crash the first n workers, where n=crashCount
		if i < crashCount {
			go runWorker(i, r, mapFunc, redFunc, partFunc, true)
		} else {
			go runWorker(i, r, mapFunc, redFunc, partFunc, false)
		}
	}

	// Block on killChan
	<-c.killChan
	// Shutdown server gracefully when termination message is received on
	// killChan
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatal("HTTP shutdown error: ", err)
	}
}

// Run Worker execution flow
func runWorker(
	index, r int,
	mapFunc, redFunc, partFunc plugin.Symbol,
	shouldCrash bool,
) {
	// Initialize Worker struct
	w := &Worker{
		workerIndex: index,
		r:           r,
		mapFunc:     mapFunc.(func(string, map[string][]string) error),
		redFunc:     redFunc.(func(string, []string, *os.File) error),
		partFunc:    partFunc.(func(string, int) int),
	}
	client, err := rpc.DialHTTP("tcp", "localhost"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Run heartbeat in the background as a goroutine
	// TODO: Try to pass a channel to this goroutine to indicate when it should
	// simulate a crash, e.g. exit--simply exiting the runWorker function does
	// *not* exit the goroutine, probably because it is actually owned by the
	// RunMapReduce function above
	go func() {
		for true {
			var hbReply bool
			err = client.Call("Coordinator.WorkerHeartbeat", &index, &hbReply)
			if err != nil {
				log.Panicf("Error sending heartbeat: %v\n", err)
			}
			// Sleep 100ms between heartbeats
			sleepInterval := time.Millisecond * 100
			time.Sleep(sleepInterval)
		}
	}()
	// Set up request args and reply variables
	args := &TaskRequest{
		WorkerIndex: w.workerIndex,
	}
	for true {
		reply := &TaskInfo{}
		err = client.Call("Coordinator.AssignTask", args, reply)
		if err != nil {
			log.Panicf("Error assigning task, assuming job is done: %v\n", err)
		}
		if reply.Stage == "finished" {
			log.Printf("MapReduce operation finished, exiting worker")
			break
		}
		if reply.Stage == "map" {
			intFiles, err := w.runMap(reply.FilesLocation[0], reply.TaskIndex)
			if err != nil {
				log.Panicf("Error completing Map Task %v (Worker %v): %v\n",
					reply.TaskIndex, w.workerIndex, err)
			}
			args.OutputFiles = intFiles
		}
		if reply.Stage == "reduce" {
			err := w.runReduce(reply.FilesLocation, reply.TaskIndex)
			if err != nil {
				log.Panicf("Error completing Reduce Task %v (Worker %v): %v\n",
					reply.TaskIndex, w.workerIndex, err)
			}
		}
		args.PrevTaskIndex = reply.TaskIndex
		args.PrevTaskStage = reply.Stage
		// If we want to crash the worker, crash after first task completed
		if shouldCrash {
			log.Printf("Worker %v crashed", w.workerIndex)
			return
		}
	}
}
