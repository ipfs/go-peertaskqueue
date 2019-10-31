package peertracker

import (
	"sync"
	"time"

	pq "github.com/ipfs/go-ipfs-pq"
	"github.com/ipfs/go-peertaskqueue/peertask"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// TaskMerger is an interface that is used to merge new tasks into the active
// and pending queues
type TaskMerger interface {
	// HasNewInfo indicates whether the given task has more information than
	// the existing group of tasks (which have the same Topic), and thus should
	// be merged.
	HasNewInfo(task peertask.Task, existing []peertask.Task) bool
	// Merge copies relevant fields from a new task to an existing task.
	Merge(task peertask.Task, existing *peertask.Task)
}

// DefaultTaskMerger is the TaskMerger used by default. It never overwrites an
// existing task (with the same Topic).
type DefaultTaskMerger struct{}

func (*DefaultTaskMerger) HasNewInfo(task peertask.Task, existing []peertask.Task) bool {
	return false
}

func (*DefaultTaskMerger) Merge(task peertask.Task, existing *peertask.Task) {
}

// PeerTracker tracks task blocks for a single peer, as well as active tasks
// for that peer
type PeerTracker struct {
	target peer.ID

	// Tasks that are pending being made active
	pendingTasks map[peertask.Topic]*peertask.QueueTask
	// Tasks that have been made active
	activeTasks map[*peertask.Task]struct{}

	// activeBytes must be locked around as it will be updated externally
	activelk    sync.Mutex
	activeBytes int

	// for the PQ interface
	index int

	freezeVal int

	// priority queue of tasks belonging to this peer
	taskQueue pq.PQ

	taskMerger TaskMerger
}

// New creates a new PeerTracker
func New(target peer.ID, taskMerger TaskMerger) *PeerTracker {
	return &PeerTracker{
		target:       target,
		taskQueue:    pq.New(peertask.WrapCompare(peertask.PriorityCompare)),
		pendingTasks: make(map[peertask.Topic]*peertask.QueueTask),
		activeTasks:  make(map[*peertask.Task]struct{}),
		taskMerger:   taskMerger,
	}
}

// PeerCompare implements pq.ElemComparator
// returns true if peer 'a' has higher priority than peer 'b'
func PeerCompare(a, b pq.Elem) bool {
	pa := a.(*PeerTracker)
	pb := b.(*PeerTracker)

	// having no pending tasks means lowest priority
	paPending := len(pa.pendingTasks)
	pbPending := len(pb.pendingTasks)
	if paPending == 0 {
		return false
	}
	if pbPending == 0 {
		return true
	}

	// Frozen peers have lowest priority
	if pa.freezeVal > pb.freezeVal {
		return false
	}
	if pa.freezeVal < pb.freezeVal {
		return true
	}

	// If each peer has an equal amount of active data in its queue, choose the
	// peer with the most amount of data waiting to send out
	if pa.activeBytes == pb.activeBytes {
		return paPending > pbPending
	}

	// Choose the peer with the least amount of active data in its queue.
	// This way we "keep peers busy" by sending them as much data as they can
	// process.
	return pa.activeBytes < pb.activeBytes
}

// Target returns the peer that this peer tracker tracks tasks for
func (p *PeerTracker) Target() peer.ID {
	return p.target
}

// IsIdle returns true if the peer has no active tasks or queued tasks
func (p *PeerTracker) IsIdle() bool {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	return len(p.pendingTasks) == 0 && len(p.activeTasks) == 0
}

// Index implements pq.Elem.
func (p *PeerTracker) Index() int {
	return p.index
}

// SetIndex implements pq.Elem.
func (p *PeerTracker) SetIndex(i int) {
	p.index = i
}

// PushTasks adds a group of tasks onto a peer's queue
func (p *PeerTracker) PushTasks(tasks []peertask.Task) {
	now := time.Now()

	p.activelk.Lock()
	defer p.activelk.Unlock()

	for _, task := range tasks {
		// If the new task doesn't add any more information over what we
		// already have in the active queue, then we can skip the new task
		if !p.taskHasMoreInfoThanActiveTasks(task) {
			continue
		}

		// If there is already a non-active task with this Topic
		if existingTask, ok := p.pendingTasks[task.Topic]; ok {
			// If the new task has a higher priority than the old task,
			if task.Priority > existingTask.Priority {
				// Update the priority and the task's position in the queue
				existingTask.Priority = task.Priority
				p.taskQueue.Update(existingTask.Index())
			}

			p.taskMerger.Merge(task, &existingTask.Task)

			// A task with the Topic exists, so we don't need to add
			// the new task to the queue
			continue
		}

		// Push the new task onto the queue
		qTask := peertask.NewQueueTask(task, p.target, now)
		p.pendingTasks[task.Topic] = qTask
		p.taskQueue.Push(qTask)
	}
}

// PopTasks pops as many tasks as possible up to the given size off the queue
// in priority order. Note that the first task is always popped, even if it's
// over maxSize, so that large tasks don't block up the queue.
func (p *PeerTracker) PopTasks(maxSize int) []*peertask.Task {
	var out []*peertask.Task
	if maxSize <= 0 {
		return out
	}

	size := 0
	for p.taskQueue.Len() > 0 && p.freezeVal == 0 {
		// Peek at the next task in the queue
		t := p.taskQueue.Peek().(*peertask.QueueTask)

		// Ignore tasks that have been cancelled
		task, ok := p.pendingTasks[t.Topic]
		if !ok {
			p.taskQueue.Pop()
			continue
		}

		// We always pop the first task off the queue, even if it's bigger
		// than the max size. This ensures that big tasks don't get stuck in
		// the queue forever.
		// After the first task, pop tasks if they fit under the max size.
		if len(out) > 0 && size+task.Size > maxSize {
			// We have as many tasks as we can fit into the message, so return
			// the tasks
			return out
		}

		// Pop the task off the queue
		p.taskQueue.Pop()

		// Start the task (this makes it "active")
		p.startTask(&task.Task)

		out = append(out, &task.Task)
		size = size + task.Size
	}

	return out
}

// startTask signals that a task was started for this peer.
func (p *PeerTracker) startTask(task *peertask.Task) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	// Remove task from pending queue
	delete(p.pendingTasks, task.Topic)

	// Add task to active queue
	if _, ok := p.activeTasks[task]; !ok {
		p.activeTasks[task] = struct{}{}
		p.activeBytes += task.Size
	}
}

// TaskDone signals that a task was completed for this peer.
func (p *PeerTracker) TaskDone(task *peertask.Task) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	// Remove task from active queue
	if _, ok := p.activeTasks[task]; ok {
		delete(p.activeTasks, task)
		p.activeBytes -= task.Size
		if p.activeBytes < 0 {
			panic("more tasks finished than started!")
		}
	}
}

// Remove removes the task with the given topic from this peer's queue
func (p *PeerTracker) Remove(topic peertask.Topic) bool {
	_, ok := p.pendingTasks[topic]
	if ok {
		delete(p.pendingTasks, topic)
	}
	return ok
}

// Freeze increments the freeze value for this peer. While a peer is frozen
// (freeze value > 0) it will not execute tasks.
func (p *PeerTracker) Freeze() {
	p.freezeVal++
}

// Thaw decrements the freeze value for this peer. While a peer is frozen
// (freeze value > 0) it will not execute tasks.
func (p *PeerTracker) Thaw() bool {
	p.freezeVal -= (p.freezeVal + 1) / 2
	return p.freezeVal <= 0
}

// FullThaw completely unfreezes this peer so it can execute tasks.
func (p *PeerTracker) FullThaw() {
	p.freezeVal = 0
}

// IsFrozen returns whether this peer is frozen and unable to execute tasks.
func (p *PeerTracker) IsFrozen() bool {
	return p.freezeVal > 0
}

// Indicates whether the new task adds any more information over tasks that are
// already in the active task queue
func (p *PeerTracker) taskHasMoreInfoThanActiveTasks(task peertask.Task) bool {
	var tasksWithTopic []peertask.Task
	for at := range p.activeTasks {
		if task.Topic == at.Topic {
			tasksWithTopic = append(tasksWithTopic, *at)
		}
	}

	// No tasks with that topic, so the new task adds information
	if len(tasksWithTopic) == 0 {
		return true
	}

	return p.taskMerger.HasNewInfo(task, tasksWithTopic)
}
