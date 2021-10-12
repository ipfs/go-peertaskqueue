package queuetracker

import (
	"sync"

	"github.com/benbjohnson/clock"
	pq "github.com/ipfs/go-ipfs-pq"
	"github.com/ipfs/go-peertaskqueue/queuetask"
)

var clockInstance = clock.New()

// TaskMerger is an interface that is used to merge new tasks into the active
// and pending queues
type TaskMerger interface {
	// HasNewInfo indicates whether the given task has more information than
	// the existing group of tasks (which have the same Topic), and thus should
	// be merged.
	HasNewInfo(task queuetask.Task, existing []queuetask.Task) bool
	// Merge copies relevant fields from a new task to an existing task.
	Merge(task queuetask.Task, existing *queuetask.Task)
}

// DefaultTaskMerger is the TaskMerger used by default. It never overwrites an
// existing task (with the same Topic).
type DefaultTaskMerger struct{}

func (*DefaultTaskMerger) HasNewInfo(task queuetask.Task, existing []queuetask.Task) bool {
	return false
}

func (*DefaultTaskMerger) Merge(task queuetask.Task, existing *queuetask.Task) {
}

// QueueTracker tracks task blocks for a single queue, as well as active tasks
// for that queue
type QueueTracker struct {
	target queuetask.Target

	// Tasks that are pending being made active
	pendingTasks map[queuetask.Topic]*queuetask.QueueTask
	// Tasks that have been made active
	activeTasks map[*queuetask.Task]struct{}

	// activeWork must be locked around as it will be updated externally
	activelk   sync.Mutex
	activeWork int

	maxActiveWorkPerQueue int

	// for the PQ interface
	index int

	freezeVal int

	queueTaskComparator queuetask.QueueTaskComparator

	// priority queue of tasks belonging to this queue
	taskQueue pq.PQ

	taskMerger TaskMerger
}

// Option is a function that configures the queue tracker
type Option func(*QueueTracker)

// WithQueueTaskComparator sets a custom QueueTask comparison function for the
// queue tracker's task queue.
func WithQueueTaskComparator(f queuetask.QueueTaskComparator) Option {
	return func(pt *QueueTracker) {
		pt.queueTaskComparator = f
	}
}

// New creates a new QueueTracker
func New(target queuetask.Target, taskMerger TaskMerger, maxActiveWorkPerQueue int, opts ...Option) *QueueTracker {
	pt := &QueueTracker{
		target:                target,
		queueTaskComparator:   queuetask.PriorityCompare,
		pendingTasks:          make(map[queuetask.Topic]*queuetask.QueueTask),
		activeTasks:           make(map[*queuetask.Task]struct{}),
		taskMerger:            taskMerger,
		maxActiveWorkPerQueue: maxActiveWorkPerQueue,
	}

	for _, opt := range opts {
		opt(pt)
	}

	pt.taskQueue = pq.New(queuetask.WrapCompare(pt.queueTaskComparator))

	return pt
}

// QueueComparator is used for queue prioritization.
// It should return true if queue 'a' has higher priority than queue 'b'
type QueueComparator func(a, b *QueueTracker) bool

// DefaultQueueComparator implements the default queue prioritization logic.
func DefaultQueueComparator(qa, qb *QueueTracker) bool {
	// having no pending tasks means lowest priority
	paPending := len(qa.pendingTasks)
	pbPending := len(qb.pendingTasks)
	if paPending == 0 {
		return false
	}
	if pbPending == 0 {
		return true
	}

	// Frozen queue have lowest priority
	if qa.freezeVal > qb.freezeVal {
		return false
	}
	if qa.freezeVal < qb.freezeVal {
		return true
	}

	// If each queue has an equal amount of work in its active queue, choose the
	// queue with the most amount of work pending
	if qa.activeWork == qb.activeWork {
		return paPending > pbPending
	}

	// Choose the queue with the least amount of work in its active queue.
	// This way we "keep queue busy" by sending them as much data as they can
	// process.
	return qa.activeWork < qb.activeWork
}

// TaskPriorityQueueComparator prioritizes queue based on their highest priority task.
func TaskPriorityQueueComparator(comparator queuetask.QueueTaskComparator) QueueComparator {
	return func(pa, pb *QueueTracker) bool {
		ta := pa.taskQueue.Peek()
		tb := pb.taskQueue.Peek()
		if ta == nil {
			return false
		}
		if tb == nil {
			return true
		}

		return comparator(ta.(*queuetask.QueueTask), tb.(*queuetask.QueueTask))
	}
}

// Target returns the queue that this queue tracker tracks tasks for
func (qt *QueueTracker) Target() queuetask.Target {
	return qt.target
}

// IsIdle returns true if the queue has no active tasks or queued tasks
func (qt *QueueTracker) IsIdle() bool {
	qt.activelk.Lock()
	defer qt.activelk.Unlock()

	return len(qt.pendingTasks) == 0 && len(qt.activeTasks) == 0
}

// QueueTrackerStats captures number of active and pending tasks for this queue.
type QueueTrackerStats struct {
	NumPending int
	NumActive  int
}

// Stats returns current statistics for this queue.
func (qt *QueueTracker) Stats() *QueueTrackerStats {
	qt.activelk.Lock()
	defer qt.activelk.Unlock()
	return &QueueTrackerStats{NumPending: len(qt.pendingTasks), NumActive: len(qt.activeTasks)}
}

// Index implements pq.Elem.
func (qt *QueueTracker) Index() int {
	return qt.index
}

// SetIndex implements pq.Elem.
func (qt *QueueTracker) SetIndex(i int) {
	qt.index = i
}

// PushTasks adds a group of tasks onto a queue's queue
func (qt *QueueTracker) PushTasks(tasks ...queuetask.Task) {
	now := clockInstance.Now()

	qt.activelk.Lock()
	defer qt.activelk.Unlock()

	for _, task := range tasks {
		// If the new task doesn't add any more information over what we
		// already have in the active queue, then we can skip the new task
		if !qt.taskHasMoreInfoThanActiveTasks(task) {
			continue
		}

		// If there is already a non-active task with this Topic
		if existingTask, ok := qt.pendingTasks[task.Topic]; ok {
			// If the new task has a higher priority than the old task,
			if task.Priority > existingTask.Priority {
				// Update the priority and the task's position in the queue
				existingTask.Priority = task.Priority
				qt.taskQueue.Update(existingTask.Index())
			}

			qt.taskMerger.Merge(task, &existingTask.Task)

			// A task with the Topic exists, so we don't need to add
			// the new task to the queue
			continue
		}

		// Push the new task onto the queue
		qTask := queuetask.NewQueueTask(task, qt.target, now)
		qt.pendingTasks[task.Topic] = qTask
		qt.taskQueue.Push(qTask)
	}
}

// PopTasks pops as many tasks off the queue as necessary to cover
// targetMinWork, in priority order. If there are not enough tasks to cover
// targetMinWork it just returns whatever is in the queue.
// The second response argument is pending work: the amount of work in the
// queue for this queue.
func (qt *QueueTracker) PopTasks(targetMinWork int) ([]*queuetask.Task, int) {
	var out []*queuetask.Task
	work := 0
	for qt.taskQueue.Len() > 0 && qt.freezeVal == 0 && work < targetMinWork {
		if qt.maxActiveWorkPerQueue > 0 {
			// Do not add work to a queue that is already maxed out
			qt.activelk.Lock()
			activeWork := qt.activeWork
			qt.activelk.Unlock()
			if activeWork >= qt.maxActiveWorkPerQueue {
				break
			}
		}

		// Pop the next task off the queue
		t := qt.taskQueue.Pop().(*queuetask.QueueTask)

		// Start the task (this makes it "active")
		qt.startTask(&t.Task)

		out = append(out, &t.Task)
		work += t.Work
	}

	return out, qt.getPendingWork()
}

// startTask signals that a task was started for this queue.
func (qt *QueueTracker) startTask(task *queuetask.Task) {
	qt.activelk.Lock()
	defer qt.activelk.Unlock()

	// Remove task from pending queue
	delete(qt.pendingTasks, task.Topic)

	// Add task to active queue
	if _, ok := qt.activeTasks[task]; !ok {
		qt.activeTasks[task] = struct{}{}
		qt.activeWork += task.Work
	}
}

func (qt *QueueTracker) getPendingWork() int {
	total := 0
	for _, t := range qt.pendingTasks {
		total += t.Work
	}
	return total
}

// TaskDone signals that a task was completed for this queue.
func (qt *QueueTracker) TaskDone(task *queuetask.Task) {
	qt.activelk.Lock()
	defer qt.activelk.Unlock()

	// Remove task from active queue
	if _, ok := qt.activeTasks[task]; ok {
		delete(qt.activeTasks, task)
		qt.activeWork -= task.Work
		if qt.activeWork < 0 {
			panic("more tasks finished than started!")
		}
	}
}

// Remove removes the task with the given topic from this queue's queue
func (qt *QueueTracker) Remove(topic queuetask.Topic) bool {
	t, ok := qt.pendingTasks[topic]
	if ok {
		delete(qt.pendingTasks, topic)
		qt.taskQueue.Remove(t.Index())
	}
	return ok
}

// Freeze increments the freeze value for this queue. While a queue is frozen
// (freeze value > 0) it will not execute tasks.
func (qt *QueueTracker) Freeze() {
	qt.freezeVal++
}

// Thaw decrements the freeze value for this queue. While a queue is frozen
// (freeze value > 0) it will not execute tasks.
func (qt *QueueTracker) Thaw() bool {
	qt.freezeVal -= (qt.freezeVal + 1) / 2
	return qt.freezeVal <= 0
}

// FullThaw completely unfreezes this queue so it can execute tasks.
func (qt *QueueTracker) FullThaw() {
	qt.freezeVal = 0
}

// IsFrozen returns whether this queue is frozen and unable to execute tasks.
func (qt *QueueTracker) IsFrozen() bool {
	return qt.freezeVal > 0
}

// Indicates whether the new task adds any more information over tasks that are
// already in the active task queue
func (qt *QueueTracker) taskHasMoreInfoThanActiveTasks(task queuetask.Task) bool {
	var tasksWithTopic []queuetask.Task
	for at := range qt.activeTasks {
		if task.Topic == at.Topic {
			tasksWithTopic = append(tasksWithTopic, *at)
		}
	}

	// No tasks with that topic, so the new task adds information
	if len(tasksWithTopic) == 0 {
		return true
	}

	return qt.taskMerger.HasNewInfo(task, tasksWithTopic)
}
