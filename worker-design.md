# Worker Design
## Map Execution Component
We take in the data string, perform the Map operation, and write the resulting
key, value pairs to R intermediate files, one for each Reduce task. We partition
these key, value pairs by key, using the partitioning function supplied by the 
user application. 

We will write to these intermediate files to disk before we return a success 
from each Map task. This is to prevent data from being lost (e.g. on worker 
crash) after the coordinator already thinks the Map task is done. Note that the
paper says we can buffer these pairs in memory and periodically write to disk
(see point #4 in 3.1), but this seems to allow for the undesired case summarized
previously.

NOTE: Not sure where to put the stuff below, putting here for now.
Multiple workers could be working on the same Map task, which means we need some
way of keeping their intermediate files separate. If we don't do this, we could
have multiple workers writing to the same set of intermediate files, which will
clearly result in race conditions. See the section of 3.2 in the paper entitled 
"Semantics in the Presence of Failures" for the paper's take on this.

The paper suggests using private temporary files per Map worker. It's unclear 
how the authors intended this to be implemented, but we can simply add a unique 
file prefix per Map worker. For example, instead of the naive mr-X-Y (where X
is the Map task index and Y a Reduce task index), we could name the intermediate
files for worker N workerN-X-Y. This will keep intermediate files for multiple
workers working on the same Map task distinct. We then need to pass the 
completed set of files to the coordinator, which will save them in its 
intermediateFiles slice. Any worker in the future that completes the same Map
task will simply be ignored.

We should also put these files into a temp directory with Go's ioutil.TempFile
and ensure that we clean them up when MapReduce completes. This will also help
with debugging the program.

## Reduce Execution Component
