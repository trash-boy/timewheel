package timewheel

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type TaskElement struct {
	task func() //任务
	pos int //slot的位置
	cycle int //时间轮的圈数
	key string //唯一的key
}

type TimeWheel struct {
	sync.Once
	interval     time.Duration           //slot所占据的时间
	ticker       *time.Ticker            //定时前进
	stopCh       chan struct{}           //推出的channel
	addTaskCh    chan *TaskElement       //添加任务的channel
	removeTaskCh chan string             //删除任务的channel
	slots        []*list.List            //双向链表来表示每一个slot的任务队列
	currSlot     int                     ///代表当前slot的位置
	keyToETask   map[string]*list.Element //为了快速在双向队列中删除对应的元素O(1)
}

func NewTimeWheel(slotNum int, interval time.Duration)*TimeWheel{
	if slotNum <= 0{
		slotNum = 10
	}

	if interval <= 0{
		interval = time.Second
	}

	t := TimeWheel{
		interval: interval,
		ticker: time.NewTicker(interval),
		stopCh: make(chan struct{}),
		keyToETask: map[string]*list.Element{},
		slots: make([]*list.List, 0, slotNum),
		addTaskCh: make(chan *TaskElement),
		removeTaskCh: make(chan string),
	}

	for i := 0 ;i < slotNum;i++{
		t.slots = append(t.slots, list.New())
	}
	go t.run()
	return &t
}


func (t *TimeWheel)Stop(){
	t.Do(func() {
		t.ticker.Stop()
		close(t.stopCh)
	})
}

func (t *TimeWheel)AddTask(key string, task func(), executeAt time.Time){
	pos,cycle := t.getPosAndCircle(executeAt)
	t.addTaskCh <- &TaskElement{
		pos: pos,
		cycle: cycle,
		task: task,
		key: key,
	}
}

func (t *TimeWheel)RemoveTask(key string){
	t.removeTaskCh <- key
}

func (t *TimeWheel)run(){
	defer func() {
		if err := recover();err != nil{
			fmt.Println(err)
		}
	}()

	for {
		select {
		case <-t.stopCh:
			t.Stop()
		case <-t.ticker.C:
			t.tick()
		case removeKey := <-t.removeTaskCh:
			t.removeTask(removeKey)
		case task := <- t.addTaskCh:
			t.addTask(task)
		}
	}
}

func (t *TimeWheel) tick() {
	list := t.slots[t.currSlot]
	defer t.circularIncr()
	t.execute(list)
}

func (t *TimeWheel) execute(l *list.List) {
	for e := l.Front();e != nil;{
		taskElement,_ := e.Value.(*TaskElement)
		if taskElement.cycle > 0{
			taskElement.cycle -= 1
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover();err != nil{
					//可以记录当前的错误
				}
			}()

			taskElement.task()
		}()

		//在list中删除，并且在map中删除
		next := e.Next()
		l.Remove(e)
		delete(t.keyToETask, taskElement.key)
		e= next
	}
}

func (t *TimeWheel) getPosAndCircle(executeAt time.Time) (int, int) {
	delay := int(time.Until(executeAt))
	cycle := delay / (len(t.slots) * int(t.interval))
	pos := (t.currSlot + delay / int(t.interval)) % len(t.slots)
	return pos,cycle
}

func (t *TimeWheel) addTask(task *TaskElement) {
	//应该先判断当前这个元素是否存在map中，如果存在首先将元素删除，然后将元素放在slot，再更新map
	ls := t.slots[task.pos]
	if _,ok := t.keyToETask[task.key];ok{
		t.removeTask(task.key)
	}
	eTask := ls.PushBack(task)
	t.keyToETask[task.key] = eTask
}

func (t *TimeWheel) removeTask(key string) {
	//通过map找到对应的task，然后分别从slot和map删除
	eTask,ok := t.keyToETask[key]
	if !ok{
		return
	}
	delete(t.keyToETask, key)
	task,_ := eTask.Value.(*TaskElement)
	_ = t.slots[task.pos].Remove(eTask)
}

func (t *TimeWheel) circularIncr() {
	t.currSlot += 1
	t.currSlot %= len(t.slots)
}


