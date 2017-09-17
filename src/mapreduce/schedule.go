package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	/*
	1. 从channel获取worker
	2. 通过worker进行rpc调用, `Worker.DoTask`,
	3. 若rpc调用执行失败, 则将任务重新塞入registerChannel执行

	ps: 使用WaitGroup保证线程同步
	若不加Wait等待所有goroutine结束在返回, 则会导致一些结果文件并未生成, 测试挂掉
 */

	var wg sync.WaitGroup
	//doneChannel := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		wg.Add(1)  // 增加WaitGroup的计数
		go func(taskNum int, n_other int, phase jobPhase) {
			debug("DEBUG: current taskNum: %v, n_other: %v, phase: %v\n", taskNum, n_other, phase)
			defer wg.Done()  // 当整个goroutine完成后, 减少引用计数
			for  {
				worker := <- registerChan  // 获取工作rpc服务器, worker == address
				debug("DEBUG: current worker port: %v\n", worker)

				var args DoTaskArgs
				args.JobName = jobName
				args.File = mapFiles[taskNum]
				args.Phase = phase
				args.TaskNumber = taskNum
				args.NumOtherPhase = n_other
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						//doneChannel <- taskNum
						registerChan <- worker // 该rpc服务器完成任务, 则重新放入registerChannel
					}()
					break  // break, 很重要, 否则一个任务会被执行多次
				}  // else 表示失败, 使用新的worker 则会进入下一次for循环重试
			}
		}(i, n_other, phase)
	}
	wg.Wait()  // 等待所有的任务完成

	fmt.Printf("Schedule: %v phase done\n", phase)
}
