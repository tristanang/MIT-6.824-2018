package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	// Creating task list
	var tasks []DoTaskArgs
	for i := 0; i < ntasks; i++ {
		task := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i], // only for map phase
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}

		tasks = append(tasks, task)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	tasksChan := make(chan int, ntasks)
	go func() {
		for i := 0; i < ntasks; i++ {
			tasksChan <- i
		}
	}()

	poolChan := make(chan string, ntasks) // Pool of available workers

	successChan := make(chan bool)

	// Function to initalize a task
	initTask := func(task int) {
		worker := <-poolChan
		success := call(worker, "Worker.DoTask", tasks[task], nil)

		if !success {
			tasksChan <- task
		} else {
			successChan <- true
		}

		poolChan <- worker
	}

	successCount := 0

	for {
		select {
		case worker := <-registerChan: // Worker initialized
			poolChan <- worker
		case task := <-tasksChan:
			go initTask(task)
		case <-successChan:
			successCount += 1
			if successCount >= ntasks {
				fmt.Printf("Schedule: %v done\n", phase)
				return
			}
		}
	}

}
