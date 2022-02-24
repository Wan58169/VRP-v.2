package main

import (
	"common"
	"fmt"
	"master"
	"os"
	"sync"
	"worker"
)

var Wg sync.WaitGroup

func init() {
	os.RemoveAll("static/results/")
	os.Mkdir("static/results", os.ModePerm)
}

func ctor(workerNum int) ([]common.Task, []chan common.Vehicle, []chan common.Task) {
	taskQ := make([]common.Task, common.TASKQSIZE)
	copy(taskQ, common.TaskQ)

	var requestsCh []chan common.Vehicle
	var replysCh []chan common.Task
	for i:=0; i<workerNum; i++ {
		requestsCh = append(requestsCh, make(chan common.Vehicle, 1))
		replysCh = append(replysCh, make(chan common.Task, 1))
	}

	return taskQ, requestsCh, replysCh
}

func run(workerNum int, a, b, c int)  {
	index := fmt.Sprintf("%d-%d-%d", a, b, c)
	common.DPrintf("index..%v start\n", index)

	/* one solution, a new lots: taskQ, channels */
	taskQ, requestsCh, replysCh := ctor(workerNum)
	/* new space to Results[index] */
	common.MapMtx.Lock()
	common.Results[index] = make([]interface{}, workerNum+1)
	common.MapMtx.Unlock()

	var handlesWg sync.WaitGroup
	handlesWg.Add(workerNum)
	go master.Run(index, workerNum, &taskQ, &requestsCh, &replysCh, &handlesWg)

	idx := 0
	/* start worker A */
	for i:=0; i<a; i++ {
		go worker.Run(index, idx, "A", requestsCh[idx], replysCh[idx])
		idx++
	}

	/* start worker B */
	for i:=0; i<b; i++ {
		go worker.Run(index, idx, "B", requestsCh[idx], replysCh[idx])
		idx++
	}

	/* start worker C */
	for i:=0; i<c; i++ {
		go worker.Run(index, idx, "C", requestsCh[idx], replysCh[idx])
		idx++
	}
	common.DPrintf("%v..all workers and handles %v start..\n", index, idx)

	handlesWg.Wait()
	common.DPrintf("%v..all workers and handles done\n", index)

	/* check master dispatch task successfully? */
	common.MapMtx.Lock()
	//common.Results[index][workerNum] = common.MasterNode{}
	mst := common.MasterNode{}
	if len(taskQ) == 0 {
		mst.SetSuccess(true)
	} else {
		mst.SetSuccess(false)
	}
	common.Results[index][workerNum] = mst
	common.MapMtx.Unlock()

	Wg.Done()
}

func main() {
	var fileName string
	var workerNum int

	fmt.Printf("input fileName and workerNum:\n")

	fmt.Scan(&fileName, &workerNum)

	/* add task to TaskQ firstly */
	common.PrepareData(fileName)
	common.FixReadyTime()

	/* start to traversal */
	for a:=0; a<=workerNum; a++ {
		for b:=0; b<=workerNum-a; b++ {
			c := workerNum-a-b

			Wg.Add(1)
			go run(workerNum, a, b, c)
		}
	}

	Wg.Wait()

	common.CalCost()

	minIndex, minCost := common.FindMinCost()
	fmt.Printf("minIndex..%v, cost..%v\n", minIndex, minCost)

	common.WriteResultsToLog()
}
