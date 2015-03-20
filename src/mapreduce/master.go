package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func AssignJob(mr *MapReduce, k int, op JobType) {
	var jobArgs *DoJobArgs
	var reply DoJobReply
	var worker string
	switch op {
	case Map:
		jobArgs = &DoJobArgs{mr.file, Map, k, mr.nReduce}
	case Reduce:
		jobArgs = &DoJobArgs{mr.file, Reduce, k, mr.nMap}
	}
	select {
	case worker = <-mr.registerChannel:
		mr.Workers[worker] = &WorkerInfo{worker}
	case worker = <-mr.idleChannel:
	}
	go func() {
		call(worker, "Worker.DoJob", jobArgs, &reply)
		mr.idleChannel <- worker
	}()
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	for i := 0; i < mr.nMap; i++ {
		AssignJob(mr, i, Map)
	}
	for i := 0; i < mr.nReduce; i++ {
		AssignJob(mr, i, Reduce)
	}
	return mr.KillWorkers()
}
