package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// Wait for at least 1 available worker
	<-mr.registerChannel

	// Worker index to keep track of the worker that takes the next task
	workerIndex := 0

	// Iterate the number of tasks to do
	for i := 0; i < ntasks; i++ {
		// Build the arguments struct
		taskArguments := DoTaskArgs{JobName: mr.jobName, File: mr.files[i], Phase: phase, TaskNumber: i, NumOtherPhase: nios}

		// Reset the worker index if it is greater than the available workers
		if workerIndex >= len(mr.workers) {
			workerIndex = 0
		}

		// Distribute the tasks in the available workers
		isCallSuccessful := call(mr.workers[workerIndex], "Worker.DoTask", taskArguments, new(struct{}))

		// If the task call failed, we have to re-schedule it
		if !isCallSuccessful {
			i--
		}

		// On each task iteration, the worker index is changed
		workerIndex++
	}

	debug("Schedule: %v phase done\n", phase)
}
