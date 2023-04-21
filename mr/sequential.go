package mr

import (
	"plugin"
)

func SequentialRun(
	inputFiles []string,
	mapFunc, redFunc, partFunc plugin.Symbol,
	kc chan int,
) {
	// TODO: Just take in input files, run the Map function on them one-by-one,
	// then run the Reduce function on each key one-by-one. No need to use
	// intermediate files, can stay entirely in memory for this. Does not need
	// to necessarily be a great performance comparison, but should be a
	// reliable baseline for us to test correctness. Should be used in any
	// automated tests as the correctness benchmark.
}
