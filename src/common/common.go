package common

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Point struct {
	X, Y int
}

var Depot Point

const (
	TaskWait = -2
	TaskNonHandle = -3
)

type Task struct {
	No int
	XY Point
	Demand int
	ReadyTime int
	DueTime int
	ServiceTime int
	NeedFix bool
	Type int
}

var TaskQ []Task
const TASKQSIZE = 100

type Vehicle struct {
	TimeStamp int
	Capacity int
	RestCap int
	XY Point
	IsOver bool
}

var Debug int

type MasterNode struct {
	success bool
	cost float32
	types []int
}

type WorkerNode struct {
	vehcType string
	points []Point
	taskNos []int
}

func (mn *MasterNode) GetSuccess() bool { return mn.success }
func (mn *MasterNode) SetSuccess(flag bool) { mn.success = flag }
func (mn *MasterNode) GetCost() float32 { return mn.cost }
func (mn *MasterNode) SetCost(cost float32) { mn.cost = cost }
func (mn *MasterNode) GetTypes() *[]int { return &mn.types }
func (mn *MasterNode) SetTypes(types []int) {
	mn.types = make([]int, len(types))
	copy(mn.types, types)
}

func (wn *WorkerNode) GetVehcType() string { return wn.vehcType }
func (wn *WorkerNode) SetVehcType(vehcType string) { wn.vehcType = vehcType }
func (wn *WorkerNode) GetPoints() []Point { return wn.points }
func (wn *WorkerNode) SetPoints(points []Point) {
	wn.points = make([]Point, len(points))
	copy(wn.points, points)
}
func (wn *WorkerNode) GetTaskNos() []int { return wn.taskNos }
func (wn *WorkerNode) SetTaskNos(nos []int) {
	wn.taskNos = make([]int, len(nos))
	copy(wn.taskNos, nos)
}

var Results map[string][]interface{}	// []Node..worker0, worker1, ..., workerN, master
var MapMtx sync.Mutex

/* model st. cost */
var KilmCosts = []float32{0.2982, 0.4, 0.67}
var DotCosts = []float32{5.7, 4.2, 2.7}

/* vehicle Type to vehicle No */
var Type2Idx map[string]int

func init() {
	Debug = 0
	TaskQ = make([]Task, 0, TASKQSIZE)
	Results = make(map[string][]interface{})
	Type2Idx = make(map[string]int)

	Type2Idx["A"] = 0
	Type2Idx["B"] = 1
	Type2Idx["C"] = 2
}

func DPrintf(format string, data ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, data...)
	}
}

func newPoint(x, y int) *Point {
	return &Point{
		X: x,
		Y: y,
	}
}

func newTask(args []int, needFix bool) *Task {
	return &Task{
		No: args[0],
		XY : Point{
			X: args[1],
			Y: args[2],
		},
		Demand: args[3],
		ReadyTime: args[4],
		DueTime: args[5],
		ServiceTime: args[6],
		NeedFix: needFix,
	}
}

func newDefaultTask(args []int) *Task {
	return newTask(args, false)
}

func NewWaitTask() *Task {
	return &Task{
		Type: TaskWait,
	}
}

/* Helper for func prepareData */
func newTaskWithRecord(record []string) *Task {
	args := make([]int, 0)

	/* conv No, X, Y, Demand, ReadyTime, DueTime, ServiceTime to int */
	for _, v := range record{
		tmp, _ := strconv.Atoi(v)
		args = append(args, tmp)
	}

	return newDefaultTask(args)
}

func PrepareData(fileName string) {
	filePath := "static/dataset/" + fileName

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("file open err\n")
	}

	reader := csv.NewReader(file)

	/* skip the title */
	reader.Read()

	/* assign Depot */
	record, _ := reader.Read()
	metaTask := newTaskWithRecord(record)
	Depot = *newPoint(metaTask.XY.X, metaTask.XY.Y)

	/* regular default task */
	context, _ := reader.ReadAll()
	for _, v := range context {
		TaskQ = append(TaskQ, *newTaskWithRecord(v))
	}
}

func FixReadyTime() {
	status := make(map[int]bool)
	fixeds := make([]Task, 0, TASKQSIZE)
	needFixs := make([]Task, 0, TASKQSIZE)

	/* fixeds & needFixs ctor, status counter */
	for _, v := range TaskQ {
		if v.NeedFix == false {
			status[v.ReadyTime] = true
			fixeds = append(fixeds, v)
		} else {
			needFixs = append(needFixs, v)
		}
	}

	/* true random */
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	/* insert the need fix task into fixeds */
	for _, v := range needFixs {
		for {
			delayTime := v.DueTime-v.ServiceTime
			readyTime := v.ReadyTime
			/* set the need fix task's readyTime */
			time := r.Int()%(delayTime-readyTime+1) + readyTime		// readyTime <= time <= delayTime

			if status[time] == false {
				v.ReadyTime = time
				v.NeedFix = false
				fixeds = append(fixeds, v)
				status[time] = true
				break
			}
		}
	}

	TaskQ = fixeds
	/* sort TaksQ based readyTime */
	sort.Slice(TaskQ, func(i, j int) bool {
		return TaskQ[i].ReadyTime < TaskQ[j].ReadyTime
	})
}

func PrintfTaskQ() {
	cnt := 0

	for _, v := range TaskQ {
		cnt++
		DPrintf("%v\n", v)
	}
	DPrintf("total %v tasks\n", cnt)
}

func CalCost() {
	var wg sync.WaitGroup

	for index, _ := range Results {
		wg.Add(1)
		go func(solution []interface{}) {
			defer wg.Done()

			master := solution[len(solution)-1].(MasterNode)
			if master.GetSuccess() {
				var cost float32
				typesNum := []int{0, 0, 0}
				workers := solution[:len(solution)-1]

				for idx, _ := range workers {
					worker := workers[idx].(WorkerNode)
					xy := Depot
					typeIdx := Type2Idx[worker.GetVehcType()]
					typesNum[typeIdx]++

					points := worker.GetPoints()
					taskNos := worker.GetTaskNos()
					/* back to depot */
					points = append(points, Depot)
					taskNos = append(taskNos, 0)

					/* kilm cost */
					for _, v := range points {
						dist := math.Abs(float64(xy.X-v.X)) + math.Abs(float64(xy.Y-v.Y))
						cost += float32(dist)*KilmCosts[typeIdx]
						xy = v
					}
					worker.SetPoints(points)
					worker.SetTaskNos(taskNos)
					workers[idx] = worker
				}

				for idx, v := range typesNum {
					cost += float32(v)*DotCosts[idx]
				}

				master.SetCost(cost)
				master.SetTypes(typesNum)
				solution[len(solution)-1] = master
			}
		}(Results[index])
	}

	wg.Wait()
}

func FindMinCost() (string, float32) {
	var minCost float32
	var minIndex string
	minCost = 1e10

	for index, solution := range Results {
		master := solution[len(solution)-1].(MasterNode)
		if master.GetSuccess() && master.GetCost()< minCost {
			minCost = master.GetCost()
			minIndex = index
		}
	}

	return minIndex, minCost
}

func WriteResultsToLog() {
	for index, solution := range Results {
		filePath := "static/results/" + index
		os.Mkdir(filePath, os.ModePerm)

		workers := solution[:len(solution)-1]
		master := solution[len(solution)-1].(MasterNode)

		var wg sync.WaitGroup
		wg.Add(len(solution)-1)

		/* log workers */
		for id, worker := range workers {
			go func(id int, filePath string, node interface{}) {
				defer wg.Done()

				fileName := "/worker" + strconv.Itoa(id) + ".csv"
				filePath += fileName
				DPrintf("filePath..%v\n", filePath)

				file, err := os.Create(filePath)
				if err != nil {
					log.Fatalf("%v create err\n", filePath)
				}
				defer file.Close()

				w := csv.NewWriter(file)

				worker := node.(WorkerNode)
				/* log vehcType */
				w.Write([]string{worker.GetVehcType()})

				points := worker.GetPoints()
				taskNos := worker.GetTaskNos()
				for idx, point := range points {
					w.Write([]string{strconv.Itoa(point.X), strconv.Itoa(point.Y), strconv.Itoa((taskNos)[idx])})
				}

				w.Flush()
			}(id, filePath, worker)
		}

		/* log master */
		fileName := "/master.csv"
		filePath += fileName
		DPrintf("filePath..%v\n", filePath)
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("%v create err\n", filePath)
		}
		defer file.Close()
		w := csv.NewWriter(file)

		var success string
		var cost float32
		if master.GetSuccess() {
			success = "success"
			cost = master.GetCost()
		} else {
			success = "failed"
			cost = 0
		}

		w.Write([]string{success, strconv.FormatFloat(float64(cost), 'f', -1, 32)})
		w.Flush()

		wg.Wait()
	}
}

