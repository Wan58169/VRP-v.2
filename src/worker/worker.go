package worker

import (
	"common"
	"time"
)

func getCapacity(vehcType string) int {
	switch vehcType {
	case "A":
		return 36
	case "B":
		return 52
	case "C":
		return 100
	default:
		return -1
	}
}

func newVehicle(capacity, restCap int, xy common.Point) *common.Vehicle {
	return &common.Vehicle{
		Capacity: capacity,
		RestCap: restCap,
		XY: xy,
	}
}

func Run(index string, id int, vehcType string, out chan<- common.Vehicle, in <-chan common.Task) {
	common.DPrintf("%v..worker %v start\n", index, id)

	capacity := getCapacity(vehcType)
	restCap := capacity

	points := make([]common.Point, 0)
	taskNos := make([]int, 0)

	/* add depot to logs firstly */
	points = append(points, common.Depot)
	taskNos = append(taskNos, 0)

	var xy common.Point
	xy = common.Depot

	request := newVehicle(capacity, restCap, xy)
	out <- *request

	for reply := range in {
		common.DPrintf("%v..reply..%v in worker %v\n", index, reply, id)

		if reply.Type == common.TaskWait {	/* retry */
			time.Sleep(1 * time.Millisecond)
		} else {
			/* regular task */
			if reply.Type == common.TaskNonHandle {	/* should back to depot */
				restCap = capacity
				xy = common.Depot

				points = append(points, common.Depot)
				taskNos = append(taskNos, 0)
			}

			xy = reply.XY
			time.Sleep(time.Millisecond * time.Duration(reply.ServiceTime))
			restCap -= reply.Demand

			points = append(points, xy)
			taskNos = append(taskNos, reply.No)
		}

		request = newVehicle(capacity, restCap, xy)
		out <- *request
	}
	close(out)

	common.MapMtx.Lock()
	//common.Results[index][id] = &common.WorkerNode{}
	worker := common.WorkerNode{}
	worker.SetVehcType(vehcType)
	worker.SetPoints(points)
	worker.SetTaskNos(taskNos)
	common.Results[index][id] = worker
	common.MapMtx.Unlock()
}
