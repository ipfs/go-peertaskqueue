package peertaskqueue

import (
	"sync"

	pq "github.com/ipfs/go-ipfs-pq"
	"github.com/ipfs/go-peertaskqueue/queuetask"
	"github.com/ipfs/go-peertaskqueue/queuetracker"
)

type taskQueueEvent int

const (
	queueAdded   = taskQueueEvent(1)
	queueRemoved = taskQueueEvent(2)
)

type hookFunc func(queueKey queuetask.Target, event taskQueueEvent)

// TaskQueue is a prioritized list of tasks to be executed on a set of queues.
// Tasks are added to the queue, then popped off alternately between queues (roughly)
// to execute the block with the highest priority, or otherwise the one added
// first if priorities are equal.
type TaskQueue struct {
	lock                       sync.Mutex
	queueComparator            queuetracker.QueueComparator
	taskComparator             queuetask.QueueTaskComparator
	pQueue                     pq.PQ
	queueTrackers              map[queuetask.Target]*queuetracker.QueueTracker
	frozenQueues               map[queuetask.Target]struct{}
	hooks                      []hookFunc
	ignoreFreezing             bool
	taskMerger                 queuetracker.TaskMerger
	maxOutstandingWorkPerQueue int
}

// Option is a function that configures the task queue
type Option func(*TaskQueue) Option

func chain(firstOption Option, secondOption Option) Option {
	return func(tq *TaskQueue) Option {
		firstReverse := firstOption(tq)
		secondReverse := secondOption(tq)
		return chain(secondReverse, firstReverse)
	}
}

// IgnoreFreezing is an option that can make the task queue ignore freezing and unfreezing
func IgnoreFreezing(ignoreFreezing bool) Option {
	return func(tq *TaskQueue) Option {
		previous := tq.ignoreFreezing
		tq.ignoreFreezing = ignoreFreezing
		return IgnoreFreezing(previous)
	}
}

// TaskMerger is an option that specifies merge behaviour when pushing a task
// with the same Topic as an existing Topic.
func TaskMerger(tmfp queuetracker.TaskMerger) Option {
	return func(tq *TaskQueue) Option {
		previous := tq.taskMerger
		tq.taskMerger = tmfp
		return TaskMerger(previous)
	}
}

// MaxOutstandingWorkPerQueue is an option that specifies how many tasks a queue can have outstanding
// with the same Topic as an existing Topic.
func MaxOutstandingWorkPerQueue(count int) Option {
	return func(tq *TaskQueue) Option {
		previous := tq.maxOutstandingWorkPerQueue
		tq.maxOutstandingWorkPerQueue = count
		return MaxOutstandingWorkPerQueue(previous)
	}
}

func removeHook(hook hookFunc) Option {
	return func(tq *TaskQueue) Option {
		for i, testHook := range tq.hooks {
			if &hook == &testHook {
				tq.hooks = append(tq.hooks[:i], tq.hooks[i+1:]...)
				break
			}
		}
		return addHook(hook)
	}
}

func addHook(hook hookFunc) Option {
	return func(tq *TaskQueue) Option {
		tq.hooks = append(tq.hooks, hook)
		return removeHook(hook)
	}
}

// OnQueueAddedHook adds a hook function that gets called whenever the tq adds a new queue
func OnQueueAddedHook(onQueueAddedHook func(target queuetask.Target)) Option {
	hook := func(target queuetask.Target, event taskQueueEvent) {
		if event == queueAdded {
			onQueueAddedHook(target)
		}
	}
	return addHook(hook)
}

// OnQueueRemovedHook adds a hook function that gets called whenever the tq adds a new queue
func OnQueueRemovedHook(onQueueRemovedHook func(target queuetask.Target)) Option {
	hook := func(target queuetask.Target, event taskQueueEvent) {
		if event == queueRemoved {
			onQueueRemovedHook(target)
		}
	}
	return addHook(hook)
}

// QueueComparator is an option that specifies custom queue prioritization logic.
func QueueComparator(pc queuetracker.QueueComparator) Option {
	return func(tq *TaskQueue) Option {
		previous := tq.queueComparator
		tq.queueComparator = pc
		return QueueComparator(previous)
	}
}

// TaskComparator is an option that specifies custom task prioritization logic.
func TaskComparator(tc queuetask.QueueTaskComparator) Option {
	return func(tq *TaskQueue) Option {
		previous := tq.taskComparator
		tq.taskComparator = tc
		return TaskComparator(previous)
	}
}

// New creates a new TaskQueue
func New(options ...Option) *TaskQueue {
	tq := &TaskQueue{
		queueComparator: queuetracker.DefaultQueueComparator,
		queueTrackers:   make(map[queuetask.Target]*queuetracker.QueueTracker),
		frozenQueues:    make(map[queuetask.Target]struct{}),
		taskMerger:      &queuetracker.DefaultTaskMerger{},
	}
	tq.Options(options...)
	tq.pQueue = pq.New(
		func(a, b pq.Elem) bool {
			pa := a.(*queuetracker.QueueTracker)
			pb := b.(*queuetracker.QueueTracker)
			return tq.queueComparator(pa, pb)
		},
	)
	return tq
}

// Options uses configuration functions to configure the queue task queue.
// It returns an Option that can be called to reverse the changes.
func (tq *TaskQueue) Options(options ...Option) Option {
	if len(options) == 0 {
		return nil
	}
	if len(options) == 1 {
		return options[0](tq)
	}
	reverse := options[0](tq)
	return chain(tq.Options(options[1:]...), reverse)
}

func (tq *TaskQueue) callHooks(to queuetask.Target, event taskQueueEvent) {
	for _, hook := range tq.hooks {
		hook(to, event)
	}
}

// TaskQueueStats captures current stats about the task queue.
type TaskQueueStats struct {
	NumQueues  int
	NumActive  int
	NumPending int
}

// Stats returns current stats about the task queue.
func (tq *TaskQueue) Stats() *TaskQueueStats {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	s := &TaskQueueStats{NumQueues: len(tq.queueTrackers)}
	for _, t := range tq.queueTrackers {
		ts := t.Stats()
		s.NumActive += ts.NumActive
		s.NumPending += ts.NumPending
	}
	return s
}

// PushTasks adds a new group of tasks for the given queue to the queue
func (tq *TaskQueue) PushTasks(to queuetask.Target, tasks ...queuetask.Task) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	queueTracker, ok := tq.queueTrackers[to]
	if !ok {
		var opts []queuetracker.Option
		if tq.taskComparator != nil {
			opts = append(opts, queuetracker.WithQueueTaskComparator(tq.taskComparator))
		}
		queueTracker = queuetracker.New(to, tq.taskMerger, tq.maxOutstandingWorkPerQueue, opts...)
		tq.pQueue.Push(queueTracker)
		tq.queueTrackers[to] = queueTracker
		tq.callHooks(to, queueAdded)
	}

	queueTracker.PushTasks(tasks...)
	tq.pQueue.Update(queueTracker.Index())
}

// PopTasks finds the queue with the highest priority and pops as many tasks
// off the queue's queue as necessary to cover targetMinWork, in priority order.
// If there are not enough tasks to cover targetMinWork it just returns
// whatever is in the queue's queue.
// - Peers with the most "active" work are deprioritized.
//   This heuristic is for fairness, we try to keep all queues "busy".
// - Peers with the most "pending" work are prioritized.
//   This heuristic is so that queues with a lot to do get asked for work first.
// The third response argument is pending work: the amount of work in the
// queue for this queue.
func (tq *TaskQueue) PopTasks(targetMinWork int) (queuetask.Target, []*queuetask.Task, int) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	if tq.pQueue.Len() == 0 {
		return "", nil, -1
	}

	// Choose the highest priority queue
	queueTracker := tq.pQueue.Peek().(*queuetracker.QueueTracker)
	if queueTracker == nil {
		return "", nil, -1
	}

	// Get the highest priority tasks for the given queue
	out, pendingWork := queueTracker.PopTasks(targetMinWork)

	// If the queue has no more tasks, remove its queue tracker
	if queueTracker.IsIdle() {
		tq.pQueue.Pop()
		target := queueTracker.Target()
		delete(tq.queueTrackers, target)
		delete(tq.frozenQueues, target)
		tq.callHooks(target, queueRemoved)
	} else {
		// We may have modified the queue tracker's state (by popping tasks), so
		// update its position in the priority queue
		tq.pQueue.Update(queueTracker.Index())
	}

	return queueTracker.Target(), out, pendingWork
}

// TasksDone is called to indicate that the given tasks have completed
// for the given queue
func (tq *TaskQueue) TasksDone(to queuetask.Target, tasks ...*queuetask.Task) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	// Get the queue tracker for the queue
	queueTracker, ok := tq.queueTrackers[to]
	if !ok {
		return
	}

	// Tell the queue tracker that the tasks have completed
	for _, task := range tasks {
		queueTracker.TaskDone(task)
	}

	// This may affect the queue's position in the queue queue, so update if
	// necessary
	tq.pQueue.Update(queueTracker.Index())
}

// Remove removes a task from the queue.
func (tq *TaskQueue) Remove(topic queuetask.Topic, target queuetask.Target) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	queueTracker, ok := tq.queueTrackers[target]
	if ok {
		if queueTracker.Remove(topic) {
			// we now also 'freeze' that partner. If they sent us a cancel for a
			// block we were about to send them, we should wait a short period of time
			// to make sure we receive any other in-flight cancels before sending
			// them a block they already potentially have
			if !tq.ignoreFreezing {
				if !queueTracker.IsFrozen() {
					tq.frozenQueues[target] = struct{}{}
				}

				queueTracker.Freeze()
			}
			tq.pQueue.Update(queueTracker.Index())
		}
	}
}

// FullThaw completely thaws all queues in the queue so they can execute tasks.
func (tq *TaskQueue) FullThaw() {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	for target := range tq.frozenQueues {
		queueTracker, ok := tq.queueTrackers[target]
		if ok {
			queueTracker.FullThaw()
			delete(tq.frozenQueues, target)
			tq.pQueue.Update(queueTracker.Index())
		}
	}
}

// ThawRound unthaws queues incrementally, so that those have been frozen the least
// become unfrozen and able to execute tasks first.
func (tq *TaskQueue) ThawRound() {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	for target := range tq.frozenQueues {
		queueTracker, ok := tq.queueTrackers[target]
		if ok {
			if queueTracker.Thaw() {
				delete(tq.frozenQueues, target)
			}
			tq.pQueue.Update(queueTracker.Index())
		}
	}
}
