package mapreduce

import (
	"os"
	"encoding/json"
	"log"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//

	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	
	decoders := make([]*json.Decoder, nMap)

	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}

		decoders[i] = json.NewDecoder(file)
		defer file.Close()
	}

	keyValues := make(map[string][]string)

	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.

	for i := 0; i < nMap; i++ {

		for {
			var kv *KeyValue
			err := decoders[i].Decode(&kv)
			if err != nil {
				break
			}

			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	var sortedKeys []string
	for key := range keyValues {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	file, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}

	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).

	enc := json.NewEncoder(file)
	for _, key := range sortedKeys {
		enc.Encode(KeyValue{key, reduceF(key, keyValues[key])})
	}
}
