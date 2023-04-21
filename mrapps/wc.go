package main

import (
	"os"
	"strconv"
	"strings"
)

// Map takes in the contents of a document as a string. It also takes in a
// pointer to a dictionary which maps intermediate keys to a list of values
// associated with it (a word to word count map, in our case). All these key,
// value pairs will eventually be written to intermediate files (writing to
// files is handled by the general Map code in our worker program).
func Map(data string, storedict map[string][]string) error {
	// Remove all punctuation from data and split by whitespace
	removePunctuation := func(r rune) rune {
		if strings.ContainsRune(".,:;!?(){}[]'`—“”’‘\"", r) {
			return -1
		} else {
			return r
		}
	}
	data = strings.Map(removePunctuation, data)
	words := strings.Fields(data)

	// Store word, count pairs in dictionary
	// NOTE: We perform a small optimization here by immediately summing the
	// counts of repeated words directly in the Map task, instead of waiting for
	// the Reduce task to do all the reducing (which is summing in this case).
	for _, w := range words {
		if val, ok := storedict[w]; ok {
			// If we already have values for a key, append to existing slice
			// NOTE: We are pre-emptively doing some reducing (see note above),
			// so instead of appending to the slice as we normally would, we
			// simply increment the existing counter.
			valInt, err := strconv.Atoi(val[0])
			if err != nil {
				return err
			}
			storedict[w][0] = strconv.Itoa(valInt + 1)
		} else {
			// If no values yet for a key, initialize a string slice for it
			storedict[w] = []string{"1"}
		}
	}
	return nil
}

// Reduce takes in a key and a list of intermediate values associated with the
// key. It also takes in a file pointer that points to the output file for this
// specific Reduce worker; Reduce will directly append its output to this output
// file.
// NOTE: Reduce expects the file pointer fp to refer to a file opened in append
// mode. If the file is not opened in append mode, Reduce will not correctly
// write data to file.
func Reduce(key string, values []string, fp *os.File) error {
	sum := 0
	for _, v := range values {
		vInt, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		sum += vInt
	}
	// Write output to output file
	kvOutput := strings.Join([]string{key, strconv.Itoa(sum)}, " ")
	_, err := fp.WriteString(kvOutput + "\n")
	if err != nil {
		return err
	}
	return nil

}

// Helper function for InputSplitter that writes a split into pg-index.txt.
func writeSplit(split string, index int) error {
	f, err := os.Create("pg-" + strconv.Itoa(index) + ".txt")
	defer f.Close()
	if err != nil {
		return err
	}
	_, err = f.WriteString(split)
	if err != nil {
		return err
	}
	return nil
}

// InputSplitter takes in a file and splits it into M separate files (one for
// each Map task). Each split file will be named pg-index, where index is some
// integer from 0 to M-1.
func InputSplitter(filename string, m int) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	dataString := string(data)
	words := strings.Fields(dataString)

	// Split input file into M separate files
	var start, index, totalWords int = 0, 0, len(words)
	for start < totalWords {
		end := start + totalWords/m
		var split string
		if end < totalWords {
			split = strings.Join(words[start:end], " ")
		} else {
			split = strings.Join(words[start:], " ")
		}
		err := writeSplit(split, index)
		if err != nil {
			return err
		}
		start = end
		index++
	}
	return nil
}

// Partitioner takes in a key and a int r that specifies the number of reduce/
// output files desired. It will then assign that key to a specific reduce task
// using some partitioning function. An int representing the reduce task number
// is returned.
func Partitioner(key string, r int) int {
	hash := func(in string) (out int) {
		out = 0
		for _, c := range in {
			out += int(c)
		}
		return
	}
	return (hash(key) % r)
}
