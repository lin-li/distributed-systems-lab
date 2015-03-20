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

func AssignJob(mr *MapReduce, jobArgs *DoJobArgs, done chan int) {
	var reply DoJobReply
	var worker string
	for {
		select {
		case worker = <-mr.registerChannel:
			mr.Workers[worker] = &WorkerInfo{worker}
		case worker = <-mr.idleChannel:
		}
		ok := call(worker, "Worker.DoJob", jobArgs, &reply)
		if (ok) {
			done <- 1
			mr.idleChannel <- worker
			return
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	done := make(chan int)
	for i := 0; i < mr.nMap; i++ {
		jobArgs := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		go AssignJob(mr, jobArgs, done)
	}
	for i := 0; i < mr.nMap; i++ { 
		<-done
	}
	for i := 0; i < mr.nReduce; i++ {
		jobArgs := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		go AssignJob(mr, jobArgs, done)
	}
	for i := 0; i < mr.nReduce; i++ { 
		<-done
	}
	return mr.KillWorkers()
}
