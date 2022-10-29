package main

import (
	"os"
	"strings"
)

// Map takes in the contents of the document as a string. It also takes in a
// pointer to a dictionary where it will store the intermediate key, value pairs
// (word, count pairs in our case). These intermediate key, value pairs will
// eventually be written from memory to files for use in Reduce.
func Map(data string, storedict *map[string]string) error {
	// Remove all punctuation from data and split by whitespace
	removePunctuation := func(r rune) rune {
		if strings.ContainsRune(".,:;", r) {
			return -1
		} else {
			return r
		}
	}
	data = strings.Map(removePunctuation, data)
	words := strings.Fields(data)
	// fmt.Println(words)

	// Store word, count pairs in dictionary
	// NOTE: We perform a small optimization here by immediately summing the
	// counts of repeated words directly in the Map task, instead of waiting for
	// the Reduce task to do all the "reducing" (which is summing in this case).
	for _, w := range words {
		if val, ok := *storedict[w]; ok {
			*storedict[w] = string(int(val) + 1)

		} else {
			*storedict[w] = "1"
		}
	}
	return nil
}

// Reduce takes in a key and a list of intermediate values associated with the
// key. It also takes in a file pointer that points to the output file for this
// specific Reduce worker; Reduce will directly append its output to this output
// file.
func Reduce(key string, values []string, fp *os.File) error {
	sum := 0
	for _, v := range values {
		sum += int(v)
	}
	// Write output to output file
	kvOutput := strings.Join([]string{key, sum}, " ")
	_, err := fp.WriteString(kvOutput + "\n")
	if err != nil {
		return err
	}
	return nil

}

// Helper function for InputSplitter that writes a split into pg-index.txt.
func writeSplit(split string, index int) error {
	f, err := os.Create("pg-" + string(index) + ".txt")
	defer f.Close()
	if err != nil {
		return err
	}
	_, err := f.WriteString(split)
	if err != nil {
		return err
	}
}

// InputSplitter takes in a file and splits it into M separate files (one for
// each Map task). Each split file will be named pg-index, where index is some
// integer from 0 to M-1.
func InputSplitter(filename string, m int) error {
	data, err := os.ReadFile(string)
	if err != nil {
		return err
	}
	data = string(data)
	words := strings.Fields(data)

	// Split input file into M separate files
	var start, index, totalWords int = 0, 0, len(words)
	for start < totalWords {
		end := start + totalWords/m
		var split string
		if end < totalWords {
			split := strings.Join(data[start:end], " ")
		} else {
			split := strings.Join(data[start:], " ")
		}
		err := writeSplit(split, index)
		if err != nil {
			return err
		}
	}
	return nil
}

// Partitioner takes in a key and a int r that specifies the number of reduce/
// output files desired. It will then assign that key to a specific reduce task
// using some partitioning function. An int representing the reduce task number
// is returned.
func Partitioner(key string, r int) (int, error) {
	hash := func(in string) (out int) {
		out = 0
		for _, c := range in {
			out += c
		}
		return
	}
	return (hash(key) % r), nil
}
