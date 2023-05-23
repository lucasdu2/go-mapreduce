# go-mapreduce
Toy implementation of MapReduce in Golang. Reference paper 
[here](https://research.google/pubs/pub62/).

## Lay of the land
A quick overview of the files in this project:
- design-doc: A set of notes used to sketch out rough implementation ideas
- mr: Implements the core MapReduce logic
- mrapps: Provides functions and inputs for MapReduce applications (we only
provide a single application: wordcount)
- run-mr.go: The code for the executable that we use to run MapReduce--it will
take the application functions and an input on the command line and run our 
MapReduce implementation with them
- test.sh: A simple testing script used to provide some basic confidence in our
implementation (for further details, see the [testing plan](design-doc/testing-plan.md))

## A brief debrief
- Some caveats and limitations:
  - No general guarantees about correctness--we only perform a very limited set
of tests in `test.sh` and the nature of our implementation (i.e the fact that
we use goroutines to simulate distributed machines) makes it difficult to 
perform more "realistic" failure injection
  - Not all important functions have docstrings (and I do not plan to spend
additional time adding them), but hopefully the inline comments, the design
notes in  `design-doc`, and the code itself are clear enough as they are
  - Many of the design decisions made here are (probably) not optimal, so please
do not use as a reference of any kind--ultimately, this is just a toy project 
for my own enjoyment/benefit
- Some final notes:
  - Be very careful about how and where you use locks in a distributed system,
they have the potential to cause very weird bugs and bad performance problems
if used irresponsibly