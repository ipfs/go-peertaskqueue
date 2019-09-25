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

	// Tasks that are pending being made active
	pendingTasks map[string]*peertask.QueueTask
	// Tasks that have been made active
	activeTasks map[string]*peertask.QueueTask

	// activeBytes must be locked around as it will be updated externally
	activelk    sync.Mutex
	activeBytes int

	// for the PQ interface
	index int

	freezeVal int

	// priority queue of tasks belonging to this peer
	taskQueue pq.PQ
}

// New creates a new PeerTracker
func New(target peer.ID) *PeerTracker {
	return &PeerTracker{
		target:       target,
		taskQueue:    pq.New(peertask.WrapCompare(peertask.PriorityCompare)),
		pendingTasks: make(map[string]*peertask.QueueTask),
		activeTasks:  make(map[string]*peertask.QueueTask),
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
		qTask := peertask.NewQueueTask(task, p.target, now)
		taskId := p.getTaskId(task.Identifier, task.IsWantBlock)

		// If the task is currently active (being processed)
		if existingTask, ok := p.anyActiveTaskWithIdentifier(task.Identifier); ok {
			// We can replace a want-have with a want-block
			replaceHaveWithBlock := !existingTask.IsWantBlock && task.IsWantBlock
			// We can replace a DONT_HAVE with a HAVE or a block
			replaceDontHave := existingTask.IsDontHave && !task.IsDontHave

			// We can only replace tasks that are not active.
			// If the active task could not have been replaced (even if it
			// wasn't active) by the new task, that means the new task is
			// not doing anything useful, so skip adding the new task.
			canReplace := replaceHaveWithBlock || replaceDontHave
			if !canReplace {
				continue
			}
		}

		// If there is already a non-active task with this Identifier
		if existingTask, ok := p.anyPendingTaskWithIdentifier(task.Identifier); ok {
			// If the new task has a higher priority than the old task,
			if task.Priority > existingTask.Priority {
				// Update the priority and the task's position in the queue
				existingTask.Priority = task.Priority
				p.taskQueue.Update(existingTask.Index())
			}

			// If we now know the size of the block, update the existing entry
			if existingTask.IsDontHave && !task.IsDontHave {
				existingTask.Size = task.Size
				existingTask.IsDontHave = false
			}

			// We can replace a want-have with a want-block
			if !existingTask.IsWantBlock && task.IsWantBlock {
				existingTaskId := p.getTaskId(existingTask.Identifier, existingTask.IsWantBlock)

				// Update the tasks's fields
				existingTask.ReplaceWith(qTask)

				// Update the mapping from task id -> task
				if taskId != existingTaskId {
					delete(p.pendingTasks, existingTaskId)
					p.pendingTasks[taskId] = existingTask
				}
			}

			// A task with the Identifier exists, so we don't need to add
			// the new task to the queue
			continue
		}

		// Push the new task onto the queue
		p.pendingTasks[taskId] = qTask
		p.taskQueue.Push(qTask)
	}
}

func (p *PeerTracker) PopTasks(maxSize int) []peertask.Task {
	var out []peertask.Task
	size := 0
	for p.taskQueue.Len() > 0 && p.freezeVal == 0 {
		// Pop a task off the queue
		task := p.taskQueue.Pop().(*peertask.QueueTask)

		// Ignore tasks that have been cancelled
		taskId := p.getTaskId(task.Identifier, task.IsWantBlock)
		if _, ok := p.pendingTasks[taskId]; !ok {
			continue
		}

		// If the next task is too big for the message
		if size+task.Size > maxSize {
			// This task doesn't fit into the message, so push it back onto the
			// queue.
			p.taskQueue.Push(task)

			// We have as many tasks as we can fit into the message, so return
			// the tasks
			return out
		}

		out = append(out, task.Task)
		size = size + task.Size

		// Start the task (this makes it "active")
		p.startTask(task)
	}

	return out
}

// startTask signals that a task was started for this peer.
func (p *PeerTracker) startTask(task *peertask.QueueTask) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	// Remove task from pending queue
	taskId := p.getTaskId(task.Identifier, task.IsWantBlock)
	delete(p.pendingTasks, taskId)

	// Add task to active queue
	if _, ok := p.activeTasks[taskId]; !ok {
		p.activeTasks[taskId] = task
		p.activeBytes += task.Size
	}
}

// TaskDone signals that a task was completed for this peer.
func (p *PeerTracker) TaskDone(identifier peertask.Identifier, isWantBlock bool) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	// Remove task from active queue
	taskId := p.getTaskId(identifier, isWantBlock)
	if task, ok := p.activeTasks[taskId]; ok {
		delete(p.activeTasks, taskId)
		p.activeBytes -= task.Size
		if p.activeBytes < 0 {
			panic("more tasks finished than started!")
		}
	}
}

// Remove removes the task with the given identifier from this peer's queue
func (p *PeerTracker) Remove(identifier peertask.Identifier) bool {
	return p.remove(identifier, true) || p.remove(identifier, false)
}

func (p *PeerTracker) remove(identifier peertask.Identifier, isWantBlock bool) bool {
	taskId := p.getTaskId(identifier, isWantBlock)
	_, ok := p.pendingTasks[taskId]
	if ok {
		delete(p.pendingTasks, taskId)
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

// anyActiveTaskWithIdentifier returns an active task with the given identifier.
func (p *PeerTracker) anyActiveTaskWithIdentifier(identifier peertask.Identifier) (*peertask.QueueTask, bool) {
	taskIdBlock := p.getTaskId(identifier, true)
	if taskBlock, ok := p.activeTasks[taskIdBlock]; ok {
		return taskBlock, true
	}

	taskIdNotBlock := p.getTaskId(identifier, false)
	if taskNotBlock, ok := p.activeTasks[taskIdNotBlock]; ok {
		return taskNotBlock, true
	}
	return nil, false
}

// anyPendingTaskWithIdentifier returns a queued task with the given identifier.
func (p *PeerTracker) anyPendingTaskWithIdentifier(identifier peertask.Identifier) (*peertask.QueueTask, bool) {
	taskIdBlock := p.getTaskId(identifier, true)
	if taskBlock, ok := p.pendingTasks[taskIdBlock]; ok {
		return taskBlock, true
	}

	taskIdNotBlock := p.getTaskId(identifier, false)
	if taskNotBlock, ok := p.pendingTasks[taskIdNotBlock]; ok {
		return taskNotBlock, true
	}
	return nil, false
}
