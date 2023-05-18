# Testing Plan
## Initial Thoughts
We should provide a baseline sequential MapReduce implementation along with the 
actual parallel implementation that we will be testing. We will test the 
sequential implementation separately and assume its correctness. 

We will need to write our own testing framework for this project, since the 
course's testing framework is not provided. The framework should consist of a 
MapReduce application (we will use wordcount), a set of inputs, and a way to
inject crashes into the worker processes. The framework should automatically run
these inputs and check if the parallel output matches the sequential output, 
which we assume to be correct.

Note that this is a fairly minimal framework design; for example, we do not plan
to check that the tasks are running in parallel (although this should already be
a given if we use goroutines), nor do we intend to try to inject failures other 
than worker crashes.  

## Specific Checks
We *will* perform following checks:
- Parallel output matches expected sequential output, even in the presence of 
worker failure
- Extraneous tasks are not scheduled if there is no worker failure

## Simulating Worker Crashes
We will simulate a worker crash by simply stopping the execution loop in the
`runWorker` function. This can be specified by a command line option that is
passed to the `runWorker` function as an argument. We should also be able to 
specify the number of workers we want to crash.
