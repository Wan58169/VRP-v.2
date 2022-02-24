package master

import (
	"common"
	"math"
	"sync"
	"time"
)

var Mtx sync.Mutex

var StartTime int

func init() {}

func dispatchTask(taskQ *[]common.Task, request *common.Vehicle) *common.Task {
	/* pick up the nearest task for worker */
	bestIdx := -1
	secondIdx := bestIdx
	bestDist := math.MaxInt32
	secondDist := bestDist

	for idx, v := range *taskQ {
		/* available task */
		if v.ReadyTime<=request.TimeStamp && v.DueTime>=request.TimeStamp && v.Demand<=request.Capacity {
			xy1 := request.XY
			xy2 := v.XY
			/* try value */
			g := int(math.Abs(float64(xy1.X-xy2.X)) + math.Abs(float64(xy1.Y-xy2.Y)))
			/* hope value */
			h := int(math.Abs(float64(xy2.X-common.Depot.X)) + math.Abs(float64(xy2.Y-common.Depot.Y)))
			/* total value */
			f := g + h

			if f <= bestDist {
				bestDist = f
				bestIdx = idx
			}
			if f<=secondDist && f>=bestDist {
				secondDist = f
				secondIdx = idx
			}
		}
	}

	if bestIdx == -1 {
		return common.NewWaitTask()
	}

	/* compare best and second */
	if bestDist==secondDist && (*taskQ)[bestIdx].Demand<(*taskQ)[secondIdx].Demand {
		bestIdx = secondIdx		/* choose the second */
	}

	task := (*taskQ)[bestIdx]
	*taskQ = append((*taskQ)[:bestIdx], (*taskQ)[bestIdx+1:]...)
	if task.Demand > request.RestCap {
		task.Type = common.TaskNonHandle
	}

	return &task
}

func isTimeOutOrTaskEnd(ticker *time.Ticker, taskQ *[]common.Task) bool {
	select {
	case <-ticker.C:
		return true
	default:
		return len(*taskQ)==0
	}
}

func workerHandle(index string, id int, taskQ *[]common.Task, out chan<- common.Task, in <-chan common.Vehicle, wg *sync.WaitGroup)  {
	common.DPrintf("%v..handle %v start\n", index, id)

	ticker := time.NewTicker(5*time.Second)

	for request := range in {
		if isTimeOutOrTaskEnd(ticker, taskQ) {
			wg.Done()
			common.DPrintf("%v..handle %v done\n", index, id)
			close(out)
			return
		}

		Mtx.Lock()
		request.TimeStamp = int(time.Now().UnixNano()/1e6)-StartTime
		task := dispatchTask(taskQ, &request)
		Mtx.Unlock()

		out <- *task
	}
}

func Run(index string, workerNum int, taskQ *[]common.Task, requestsCh *[]chan common.Vehicle, replysCh *[]chan common.Task,
	wg *sync.WaitGroup) {

	for i:=0; i<workerNum; i++ {
		go workerHandle(index, i, taskQ, (*replysCh)[i], (*requestsCh)[i], wg)
	}

	StartTime = int(time.Now().UnixNano()/1e6)

	common.DPrintf("%v..master start\n", index)
}
