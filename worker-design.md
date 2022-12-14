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
workers working on the same Map task distinct. Additionally, since this format
is deterministic, we do not need to pass the names to the coordinator in an 
RPC. Instead, the coordinator can simply assume the format and construct the 
file names itself.

We should also put all these intermediate files into a single separate directory.
This will make cleanup of these files easier upon MapReduce completion and will 
also help with debugging the program.

## Reduce Execution Component
Multiple workers can be working on the same Reduce task. This results in the 
same concurrency problem as above, where multiple workers can be writing to the
same output file if we do not specify a way to distinguish them.

We follow the advice of the MapReduce paper by creating a unique temporary output
file for each worker. When a worker finishes a Reduce task, it will attempt to
atomically rename the its temporary output file to the final output file name.
We rely on this atomic rename to guarantee that only a single, consistent output
file will be produced for each Reduce task.
