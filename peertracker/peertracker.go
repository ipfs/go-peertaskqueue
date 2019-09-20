package peertracker

import (
	"fmt"
	"sync"
	"time"

	pq "github.com/ipfs/go-ipfs-pq"
	"github.com/ipfs/go-peertaskqueue/peertask"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// PeerTracker tracks task blocks for a single peer, as well as active tasks
// for that peer
type PeerTracker struct {
	target peer.ID
	// Active is the number of track tasks this peer is currently
	// processing
	// active must be locked around as it will be updated externally
	activelk    sync.Mutex
	activeBytes int
	activeTasks map[string]struct{}

	// total number of tasks for this peer
	numTasks int

	// for the PQ interface
	index int

	freezeVal int

	// Map of task id -> task
	taskMap map[string]*peertask.QueueTask

	// priority queue of tasks belonging to this peer
	taskQueue pq.PQ
}

// New creates a new PeerTracker
func New(target peer.ID) *PeerTracker {
	return &PeerTracker{
		target:      target,
		taskQueue:   pq.New(peertask.WrapCompare(peertask.PriorityCompare)),
		taskMap:     make(map[string]*peertask.QueueTask),
		activeTasks: make(map[string]struct{}),
	}
}

// PeerCompare implements pq.ElemComparator
// returns true if peer 'a' has higher priority than peer 'b'
func PeerCompare(a, b pq.Elem) bool {
	pa := a.(*PeerTracker)
	pb := b.(*PeerTracker)

	// having no tasks means lowest priority
	// having both of these checks ensures stability of the sort
	if pa.numTasks == 0 {
		return false
	}
	if pb.numTasks == 0 {
		return true
	}

	// Frozen peers have lowest priority
	if pa.freezeVal > pb.freezeVal {
		return false
	}
	if pa.freezeVal < pb.freezeVal {
		return true
	}

	// having no pending tasks means lowest priority
	if pa.taskQueue.Len() == 0 {
		return false
	}
	if pb.taskQueue.Len() == 0 {
		return true
	}

	// Choose the peer with the least amount of active data in its queue.
	// This way we "keep peers busy" by sending them as much data as they can
	// process.
	return pa.activeBytes < pb.activeBytes
}

// startTask signals that a task was started for this peer.
func (p *PeerTracker) startTask(identifier peertask.Identifier, isWantBlock bool) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	taskId := p.getTaskId(identifier, isWantBlock)
	p.activeTasks[taskId] = struct{}{}
	task, ok := p.taskMap[taskId]
	if !ok {
		panic(fmt.Sprintf("Active task %s not found in task map", taskId))
	}
	p.activeBytes += task.Size
}

// TaskDone signals that a task was completed for this peer.
func (p *PeerTracker) TaskDone(identifier peertask.Identifier, isWantBlock bool) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	taskId := p.getTaskId(identifier, isWantBlock)
	delete(p.activeTasks, taskId)

	task, ok := p.taskMap[taskId]
	if ok && !task.Removed {
		delete(p.taskMap, taskId)
		p.numTasks--

		p.activeBytes -= task.Size
		if p.activeBytes < 0 {
			panic("more tasks finished than started!")
		}
	}
}

// Target returns the peer that this peer tracker tracks tasks for
func (p *PeerTracker) Target() peer.ID {
	return p.target
}

// IsIdle returns true if the peer has no active tasks or queued tasks
func (p *PeerTracker) IsIdle() bool {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	return p.numTasks == 0 && p.activeBytes == 0
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
		qTask := peertask.NewQueueTask(task, p.target, now)
		taskId := p.getTaskId(task.Identifier, task.IsWantBlock)

		// If the task is currently active (being processed)
		if isActiveTaskAWantBlock, ok := p.isAnyTaskWithIdentifierActive(task.Identifier); ok {
			// We can only replace tasks that are not active.
			// If the active task could not have been replaced (even if it
			// wasn't active) by the new task, that means the new task is
			// not doing anything useful, so skip adding the new task.
			if isActiveTaskAWantBlock || !task.IsWantBlock {
				continue
			}
		} else if existingTask, ok := p.anyQueuedTaskWithIdentifier(task.Identifier); ok {
			// There is already a task with this Identifier, and the task is not active

			// If the new task has a higher priority than the old task,
			if task.Priority > existingTask.Priority {
				// Update the priority and the tasks position in the queue
				existingTask.Priority = task.Priority
				p.taskQueue.Update(existingTask.Index())
			}

			// If we now know the size of the block, update the existing entry
			if existingTask.IsDontHave && !task.IsDontHave {
				existingTask.Size = task.Size
			}

			// We can replace a want-have with a want-block
			if !existingTask.IsWantBlock && task.IsWantBlock {
				existingTaskId := p.getTaskId(existingTask.Identifier, existingTask.IsWantBlock)

				// Update the tasks's fields
				existingTask.ReplaceWith(qTask)

				// Update the mapping from task id -> task
				if taskId != existingTaskId {
					delete(p.taskMap, existingTaskId)
					p.taskMap[taskId] = existingTask
				}
			}

			// A task with the Identifier exists, so we don't need to add
			// the new task to the queue
			continue
		}

		// Push the new task onto the queue
		p.taskMap[taskId] = qTask
		p.numTasks++
		p.taskQueue.Push(qTask)
	}
}

func (p *PeerTracker) PopTasks(maxSize int) []peertask.Task {
	var out []peertask.Task
	size := 0
	for p.taskQueue.Len() > 0 && p.freezeVal == 0 {
		// Pop a task off the queue
		task := p.taskQueue.Pop().(*peertask.QueueTask)

		// If it's been pruned, skip it
		if task.Removed {
			delete(p.taskMap, p.getTaskId(task.Identifier, task.IsWantBlock))
			continue
		}

		// If the next task is too big for the message
		if size+task.Size > maxSize {
			// This task doesn't fit into the message, so push it back onto the
			// queue.
			p.taskQueue.Push(task)

			return out
		}

		out = append(out, task.Task)
		size = size + task.Size

		// Start the task (this makes it "active")
		p.startTask(task.Identifier, task.IsWantBlock)
	}

	return out
}

// Remove removes the task with the given identifier from this peer's queue
func (p *PeerTracker) Remove(identifier peertask.Identifier) bool {
	return p.remove(identifier, true) || p.remove(identifier, false)
}

func (p *PeerTracker) remove(identifier peertask.Identifier, isWantBlock bool) bool {
	taskId := p.getTaskId(identifier, isWantBlock)
	task, ok := p.taskMap[taskId]
	if ok && !task.Removed {
		// Note that we don't actually remove the reference from p.taskMap
		// because the task is still in p.taskQueue. It will eventually get
		// popped off the queue and cleaned up (in PopTasks())
		task.Removed = true
		p.numTasks--
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

// getTaskId gets the task id for a task with the given identifier, of the given type.
func (p *PeerTracker) getTaskId(identifier peertask.Identifier, isWantBlock bool) string {
	return fmt.Sprintf("%s-%t", identifier, isWantBlock)
}

// isAnyTaskWithIdentifierActive indicates if there is an active task with the
// given identifier. The first return argument indicates the type:
// true:  want-block
// false: want-have
func (p *PeerTracker) isAnyTaskWithIdentifierActive(identifier peertask.Identifier) (bool, bool) {
	taskIdBlock := p.getTaskId(identifier, true)
	if _, ok := p.activeTasks[taskIdBlock]; ok {
		return true, true
	}

	taskIdNotBlock := p.getTaskId(identifier, false)
	if _, ok := p.activeTasks[taskIdNotBlock]; ok {
		return false, true
	}
	return false, false
}

// anyQueuedTaskWithIdentifier returns a queued task with the given identifier.
func (p *PeerTracker) anyQueuedTaskWithIdentifier(identifier peertask.Identifier) (*peertask.QueueTask, bool) {
	taskIdBlock := p.getTaskId(identifier, true)
	if taskBlock, ok := p.taskMap[taskIdBlock]; ok {
		return taskBlock, true
	}

	taskIdNotBlock := p.getTaskId(identifier, false)
	if taskNotBlock, ok := p.taskMap[taskIdNotBlock]; ok {
		return taskNotBlock, true
	}
	return nil, false
}
