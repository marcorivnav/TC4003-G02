package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.

	// Open the files generated by common_map and store them in filesSystem
	var filesSystem []*os.File
	for i := 0; i < nMap; i++ {
		// Generator of the names of files:
		file, _ := os.Open(reduceName(jobName, i, reduceTaskNumber))
		filesSystem = append(filesSystem, file)
	}

	// Decode the files stored in filesSystem.
	cachedKV := make(map[string][]string)

	// Going to iterate over every file of filesSystem
	for _, file := range filesSystem {
		// Close until functions returns.
		defer file.Close()
		// Decode JSON file
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// Break until there are no more entries.
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// Store entries in cachedKV.
			cachedKV[kv.Key] = append(cachedKV[kv.Key], kv.Value)
		}
	}

	// From type map to a slice of strings.
	var keys []string
	for key := range cachedKV {
		keys = append(keys, key)
	}

	// Sort the entries.
	sort.Strings(keys)

	// Create new file to store the results.
	file, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkError(err)
	defer file.Close()

	// Encode keys in the generated file (where results should be).
	enc := json.NewEncoder(file)
	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, cachedKV[key])})
	}
}