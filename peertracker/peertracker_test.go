package peertracker

import (
	"testing"

	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/testutil"
)

func TestEmpty(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	if len(tracker.PopTasks(100)) != 0 {
		t.Fatal("Expected no tasks")
	}
}

func TestPushPop(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 1,
			Size:     10,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "1" {
		t.Fatal("Expected same task")
	}
}

func TestPopNegativeOrZeroSize(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 1,
			Size:     10,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(-1)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
	popped = tracker.PopTasks(0)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
}

func TestPushPopSizeAndOrder(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
		},
		peertask.Task{
			Topic:    "2",
			Priority: 20,
			Size:     10,
		},
		peertask.Task{
			Topic:    "3",
			Priority: 15,
			Size:     10,
		},
	}
	tracker.PushTasks(tasks)

	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected tasks in order")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Topic != "3" || popped[1].Topic != "1" {
		t.Fatal("Expected tasks in order")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
}

func TestPushPopFirstItemOversized(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
		},
		peertask.Task{
			Topic:    "2",
			Priority: 10,
			Size:     5,
		},
	}
	tracker.PushTasks(tasks)

	// Pop with max size 7.
	// PopTasks should always return the first task even if it's over max size
	// (this is to prevent large tasks from blocking up the queue).
	popped := tracker.PopTasks(7)
	if len(popped) != 1 || popped[0].Topic != "1" {
		t.Fatal("Expected first task to be popped")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
}

func TestRemove(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
		},
		peertask.Task{
			Topic:    "2",
			Priority: 20,
			Size:     10,
		},
		peertask.Task{
			Topic:    "3",
			Priority: 15,
			Size:     10,
		},
	}
	tracker.PushTasks(tasks)
	tracker.Remove("2")
	popped := tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Topic != "3" || popped[1].Topic != "1" {
		t.Fatal("Expected tasks in order")
	}
}

func TestRemoveMulti(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
		},
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     1,
		},
		peertask.Task{
			Topic:    "2",
			Priority: 15,
			Size:     10,
		},
	}
	tracker.PushTasks(tasks)
	tracker.Remove("1")
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected remaining task")
	}
}

func TestTaskDone(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Pop task "a". This makes the task active.
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Mark task "a" as done.
	tracker.TaskDone(popped[0])

	// Push task "b"
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "1"

	// Pop all tasks. Task "a" was done so task "b" should have been allowed to
	// be added.
	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
}

type permissiveTaskMerger struct{}

func (*permissiveTaskMerger) HasNewInfo(task peertask.Task, existing []peertask.Task) bool {
	return true
}
func (*permissiveTaskMerger) Merge(task peertask.Task, existing *peertask.Task) {
	existing.Data = task.Data
	existing.Size = task.Size
}

func TestReplaceTaskPermissive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Push task "b". Has same topic and permissive task merger, so should
	// replace task "a".
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "1"

	// Pop all tasks, should only be task "b".
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Data != "b" {
		t.Fatal("Expected b to replace a")
	}
	if popped[0].Priority != 20 {
		t.Fatal("Expected higher Priority to replace lower Priority")
	}
}

func TestReplaceTaskSize(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "2",
			Priority: 10,
			Size:     10,
			Data:     "b",
		},
		peertask.Task{
			Topic:    "2",
			Priority: 10,
			Size:     30,
			Data:     "c",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Push task "b"
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "2"

	// Push task "c". Has same topic as task "b" and permissive task merger,
	// so should replace task "b", and update its size
	tracker.PushTasks([]peertask.Task{tasks[2]}) // Topic "2"

	// Pop with maxSize 20. Should only pop task "a" because only other task
	// (with Topic "2") now has size 30.
	popped := tracker.PopTasks(20)
	if len(popped) != 1 || popped[0].Data != "a" {
		t.Fatal("Expected 1 task", popped[0], popped[1])
	}
	popped = tracker.PopTasks(30)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
}

func TestReplaceActiveTask(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Pop task "a". This makes the task active.
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b"
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "1"

	// Pop all tasks. Task "a" was active so task "b" should have been moved to
	// the pending queue.
	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
}

func TestReplaceActiveTaskNonPermissive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Pop task "a". This makes the task active.
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b". Task merger is not permissive, so should ignore task "b".
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "1"

	// Pop all tasks.
	popped = tracker.PopTasks(100)
	if len(popped) != 0 {
		t.Fatal("Expected no tasks")
	}
}

func TestReplaceTaskThatIsActiveAndPending(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "b",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "c",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Pop task "a". This makes the task active.
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b". Same Topic so should be added to the pending queue.
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "1"

	// Push task "c". Permissive task merger so should replace pending task "b"
	// with same Topic.
	tracker.PushTasks([]peertask.Task{tasks[2]}) // Topic "1"

	// Pop all tasks.
	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Data != "c" {
		t.Fatalf("Expected last task to overwrite pending task")
	}
}

func TestRemoveActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{})

	tasks := []peertask.Task{
		peertask.Task{
			Topic:    "1",
			Priority: 10,
			Size:     10,
			Data:     "a",
		},
		peertask.Task{
			Topic:    "1",
			Priority: 20,
			Size:     10,
			Data:     "b",
		},
		peertask.Task{
			Topic:    "2",
			Priority: 15,
			Size:     10,
			Data:     "c",
		},
	}

	// Push task "a"
	tracker.PushTasks([]peertask.Task{tasks[0]}) // Topic "1"

	// Pop task "a". This makes the task active.
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b" and "c"
	tracker.PushTasks([]peertask.Task{tasks[1]}) // Topic "1"
	tracker.PushTasks([]peertask.Task{tasks[2]}) // Topic "2"

	// Remove all tasks with Topic "1".
	// This should remove task "b" from the pending queue.
	tracker.Remove("1")
	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected tasks in order")
	}
}
