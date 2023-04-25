# Testing Plan
## 4.25.23 Rough Idea
For correctness, we can dynamically generate the expected output using the 
sequential implementation and compare it to the output from the real, parallel 
MapReduce implementation. We could also hard-code the expected outputs for some
set of inputs, but I think the dynamic approach is cleaner.

The most complex thing about the testing framework is to figure out how to
simulate failures. We want to simulate 2 basic failures: 1) worker crashes, 2)
worker-coordinator network partitions (where worker is still alive but cannot 
communicate with coordinator). 

To simulate both these failures, the easiest thing to do is the add the option
to insert these failures into the code itself, specifically into the code that 
executes each worker in `mapreduce.go` (aka the `runWorker` function). To
simulate worker failure, we can add some code in the worker's main execution 
loop that randomly exits the worker. To simulate worker-coordinator network 
partitions, we need to find a way to pause the worker's RPC requests (both 
the heartbeat and any AssignTask requests) temporarily. 

We should also be able to dynamically configure some settings regarding these
failures. For example, we should be able to set the odds of a worker failing
and the amount of time that a simulated partition should last. We may be able 
to do this by adding a plugin that contains a struct with these configurable
settings as fields. A user would then be able to customize them at compile-time.
