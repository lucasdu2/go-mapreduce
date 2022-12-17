# Coordinator Design
## Without Backup Tasks
First, we set up the basic outline of the coordinator, without implementing
Backup Tasks (as specified in 3.6 of the MapReduce paper). We can later extend
this design to support Backup Tasks. 

### Coordinator Data Structures
TODO: summarize data structures inside the Coordinator struct, what each is for

### Coordinating Stage Completion
One major issue to be addressed is how to coordinate when a stage (either Map
or Reduce) is finished. We want to only start Reduce tasks when all Map tasks
are finished and we want to return the output only when all Reduce tasks are 
finished. We track the completion of a stage using an atomic counter, which 
we have called taskCounter. But how can we coordinate worker behavior when 
taskCounter tells us that a stage is finished? 

First, let's define the "waiting state" of a worker to be when there are no more
tasks to be assigned to it, but the stage is not yet complete. There are two main 
options to manage workers in the waiting state. 

1) Have the workers in the waiting state continuously ask the coordinator for a 
task (with some time between asks). As long as the stage is unfinished, the 
coordinator will return no further tasks in response. Once the stage is finished, 
the coordinator will assign Reduce tasks in response, or will indicate that the 
MapReduce operation as a whole has completed.

2) Wait inside the specific RPC handler for the worker in the waiting state. When 
the stage is complete, we will broadcast to all waiting RPC handlers that they
can stop waiting and continue. Go provides the sync.Cond type, which allows us 
to easily implement this kind of wait. 

In this MapReduce implementation, we choose the 2nd option. In theory, this 
option is more efficient in terms of both time and network usage. The former
option results in constant network usage as workers in the waiting state 
continuously request a new task from the coordinator. The 2nd option simply 
waits inside the coordinator's RPC handler, so a waiting worker will not generate
any network traffic. Additionally, the 2nd option allows us to almost instantly
broadcast that the current stage is over and move to the Reduce stage/end the
MapReduce operation. In contrast, the 1st option requires the workers to submit
an RPC to the coordinator to find out that the stage is over. Depending on the
particular timings of the system, there may be some latency after the coordinator
knows the stage is over before the workers are able to proceed.

### Coordinating Access to Intermediate Files
Each Map task will write to a set of intermediate files of format workerN-X-Y, 
where X is the index of the Map task, Y is the index of a Reduce task, and N is
the index of the worker that completed the task. 

Upon the completion of a Map task, the coordinator will append the new file for 
each Reduce task to the slice at the appropriate index in intermediateFiles (see
Coordinator struct for intermediateFiles definition). This will automatically 
collect the set of files for each Reduce task in discrete slices, which makes 
the process of assigning them easier.

We need to protect the intermediateFiles slice with a lock because multiple
different workers can concurrently update it, and specifically, can update the
same indices within it. This means that multiple threads can be writing to the
same data, which will lead to correctness issues without a lock to enforce
sequential execution of some kind. 

On a related note, the workers slice (see Coordinator struct) can also be 
updated concurrently by multiple workers. Specifically, multiple workers can
concurrently be assigned a task by calling AssignTask, which will also 
concurrently update the workers slice. However, each index of workers will only
ever be updated by a single worker. And due to the nature of MapReduce, where 
there is no recourse in the case of coordinator failure, the worker will never 
have reason to issue overlapping calls to AssignTask. So a worker will only 
call AssignTask, and thus update its workers index, sequentially. Thus, there is
actually no correctness problem due to concurrency in workers, since each thread 
will only update its own piece of data in sequential order.

