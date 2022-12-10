# Design
## Calling Code
The calling code in main.go should handle all the prep work for the MapReduce
operation and should also offer an interface for the user to execute an 
arbitrary MapReduce application. Specifically, it should handle the following
inputs:
- Input file
- `.so` file containing all application-defined plugin functions
- Number of workers available
- Number of Map tasks (M)
- Number of Reduce tasks (R)
The calling code should split the input file into M pieces (one for each Map
task) and should spawn the coordinator and worker processes. Additionally, we
should load the user-created Map and Reduce functions at run-time using the Go
plugin package (which looks for files ending in `.so`). 

The calling code should also be able to take the R output files (from the
Reduce tasks) and produce a single, combined output file. 

### Additional Details
**[10-31-22]**
The application will need to define 4 plugin functions. It must define a Map and 
Reduce function that will be called for Map and Reduce tasks, respectively. It 
must also define an InputSplitter function (to specify how the application wants 
to split the single input file into M splits for the Map stage), and a Partitioner
function to define how intermediate keys are partitioned into R groups for the
Reduce stage. These functions will be loaded at run-time using a plugin. Note:
InputSplitter must split up the input file into files of name pg-{index}.txt,
where {index} is in [0, M). The coordinator code will expect this format. 

## Coordinator
The coordinator should be implemented as an RPC server--the workers and the 
coordinator will communicate using RPCs. The coordinator has several
responsibilities, which are listed below:
- Hand out tasks to free workers
- Periodically check worker health, respond if worker is unhealthy
- Coordinate and track the progress of the MapReduce operation
    - Ensure all Map tasks are completed before starting the Reduce tasks
    - Store the locations, size of the R intermediate files (from the Map tasks)
    - For each task, it stores task state and identity of worker machine
Note that our coordinator implementation will not deal with its own failure. 
As described in the original MapReduce paper, if the coordinator fails during a
MapReduce operation, the operation will simply terminate. 

### Additional Details
See the specific coordinator design document [here](coordinator-design.md).

## Worker
A worker will communicate with the coordinator using RPCs. In general, a worker
should have two main components. The communication component, where it will ask
talk to the coordinator, and the execution component, where it will actually
execute the Map or Reduce task given to it. 

### Communication Component
Whenever a worker is idle, it will ask the coordinator for its next task. 
Additionally, the coordinator will periodically send a heartbeat to the worker;
if the worker is still alive, it must send a response back.

### Execution Component
The worker's primary function is to execute the Map or Reduce task that is
assigned to it by the coordinator. For Map tasks, it will take the file for 
the Map task and run the Map function on it. The key/value pairs that are
produced must be partitioned into R regions and stored somewhere for the Reduce 
tasks later. The partitioning algorithm (its design and implementation) may be 
a point of difficulty; it might be sufficient to use a hashing function of some
kind. The locations of these R regions must be passed back to the coordinator,
which will then be able to give it to Reduce workers. 

For Reduce tasks, the worker will get its assigned region (which holds the 
intermediate data) from the coordinator, read the data, and sort the data so
that all occurrences of the same key are grouped together. Then it will iterate
over the sorted data and for each unique key encountered, it will pass the 
corresponding set of intermediate values to the Reduce function. The result of 
the Reduce function will be appended to the final output file for this specific
Reduce task. 

# Testing Plan
You will need to write your own testing framework for this project, since the 
course's testing framework is not provided. The framework should consist of a
MapReduce application (along with a set of inputs and expected outputs) and a 
way to inject crashes into the worker processes. Ideally, the framework should
be able to automatically run these inputs and check if the output matches the
expected output. The runtime should also be recorded for performance comparisons.

The simplest test case may be a word count application. As noted above, you will 
need to implement the Map and Reduce functions for this application. You will 
also need to supply several (2 or 3) input files along with their corresponding 
expected outputs. 

Note that this is a fairly minimal framework design; we do not plan to check 
that the tasks are running in parallel, for example (although parallelism should 
already be a given if we use goroutines), nor do we plan to check that 
extraneous tasks are not scheduled if there is no worker failure (as mentioned
in the lab handout). The primary focus of this project should be on implementing
MapReduce itself. The testing framework is additional work that needs to be done
because it is not provided freely online, but does not need to be particularly 
rigorous in our case. 
