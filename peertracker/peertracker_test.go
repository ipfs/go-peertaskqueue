package peertracker

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/testutil"
)

const testMaxActiveWorkPerPeer = 100

func TestEmpty(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks, _ := tracker.PopTasks(100)
	if len(tasks) != 0 {
		t.Fatal("Expected no tasks")
	}
}

func TestPushPop(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 1,
			Work:     10,
		},
	}
	tracker.PushTasks(tasks...)

	tracker.PushTasksTruncated(1, peertask.Task{
		Topic:    "2",
		Priority: 2,
		Work:     20,
	})

	popped, _ := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "1" {
		t.Fatal("Expected same task")
	}
}

func TestPopNegativeOrZeroSize(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 1,
			Work:     10,
		},
	}
	tracker.PushTasks(tasks...)
	popped, _ := tracker.PopTasks(-1)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
	popped, _ = tracker.PopTasks(0)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
}

func TestPushPopSizeAndOrder(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
		},
		{
			Topic:    "2",
			Priority: 20,
			Work:     10,
		},
		{
			Topic:    "3",
			Priority: 15,
			Work:     10,
		},
	}
	tracker.PushTasks(tasks...)

	popped, pending := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected tasks in order")
	}
	if pending != 20 {
		t.Fatal("Expected pending work to be 20")
	}

	topics := tracker.Topics()
	if len(topics.Active) != 1 || topics.Active[0] != popped[0].Topic {
		t.Fatal("Expected 1 active topic with popped task's topic")
	}
	if len(topics.Pending) != 2 {
		t.Fatal("Expected pending topics to be 2")
	}
	if !(topics.Pending[0] == "1" || topics.Pending[1] == "1") {
		t.Fatal("Missing pending topic")
	}
	if !(topics.Pending[0] == "3" || topics.Pending[1] == "3") {
		t.Fatal("Missing pending topic")
	}

	popped, pending = tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Topic != "3" || popped[1].Topic != "1" {
		t.Fatal("Expected tasks in order")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}

	topics = tracker.Topics()
	if len(topics.Active) != 3 {
		t.Fatal("Expected 3 active topics")
	}
	if len(topics.Pending) != 0 {
		t.Fatal("Expected no pending topics")
	}
	stringTopics := []string{topics.Active[0].(string), topics.Active[1].(string), topics.Active[2].(string)}
	sort.Strings(stringTopics)
	if !reflect.DeepEqual(stringTopics, []string{"1", "2", "3"}) {
		t.Fatal("Expected active topics to be 1, 2, 3")
	}

	popped, pending = tracker.PopTasks(100)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}
}

func TestPopFirstItemAlways(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 20,
			Work:     10,
		},
		{
			Topic:    "2",
			Priority: 10,
			Work:     5,
		},
	}
	tracker.PushTasks(tasks...)

	// Pop with target size 7.
	// PopTasks should always return the first task even if it's under target work.
	popped, _ := tracker.PopTasks(7)
	if len(popped) != 1 || popped[0].Topic != "1" {
		t.Fatal("Expected first task to be popped")
	}

	popped, _ = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
}

func TestPopItemsToCoverTargetWork(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 20,
			Work:     5,
		},
		{
			Topic:    "2",
			Priority: 10,
			Work:     5,
		},
		{
			Topic:    "3",
			Priority: 5,
			Work:     5,
		},
	}
	tracker.PushTasks(tasks...)

	// Pop with target size 7.
	// PopTasks should return enough items to cover the target work.
	popped, _ := tracker.PopTasks(7)
	if len(popped) != 2 || popped[0].Topic != "1" || popped[1].Topic != "2" {
		t.Fatal("Expected first two tasks to be popped")
	}

	popped, _ = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
}

func TestRemove(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
		},
		{
			Topic:    "2",
			Priority: 20,
			Work:     10,
		},
		{
			Topic:    "3",
			Priority: 15,
			Work:     10,
		},
	}
	tracker.PushTasks(tasks...)
	tracker.Remove("2")
	popped, _ := tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Topic != "3" || popped[1].Topic != "1" {
		t.Fatal("Expected tasks in order")
	}
}

func TestRemoveMulti(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
		},
		{
			Topic:    "1",
			Priority: 20,
			Work:     1,
		},
		{
			Topic:    "2",
			Priority: 15,
			Work:     10,
		},
	}
	tracker.PushTasks(tasks...)
	tracker.Remove("1")
	popped, _ := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected remaining task")
	}
}

func TestTaskDone(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 20,
			Work:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Check topic state
	topics := tracker.Topics()
	if len(topics.Active) != 0 {
		t.Fatal("Expected no active topics")
	}
	if len(topics.Pending) != 1 {
		t.Fatal("Expected 1 pending topics")
	}

	// Pop task "a". This makes the task active.
	popped, _ := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Check topic state
	topics = tracker.Topics()
	if len(topics.Active) != 1 {
		t.Fatal("Expected 1 active topics")
	}
	if len(topics.Pending) != 0 {
		t.Fatal("Expected no pending topics")
	}

	// Mark task "a" as done.
	tracker.TaskDone(popped[0])

	// Check topic state
	topics = tracker.Topics()
	if len(topics.Active) != 0 {
		t.Fatal("Expected no active topics")
	}
	if len(topics.Pending) != 0 {
		t.Fatal("Expected no pending topics")
	}

	// Push task "b"
	tracker.PushTasks(tasks[1]) // Topic "1"

	// Check topic state
	topics = tracker.Topics()
	if len(topics.Active) != 0 {
		t.Fatal("Expected no active topics")
	}
	if len(topics.Pending) != 1 {
		t.Fatal("Expected 1 pending topics")
	}

	// Pop all tasks. Task "a" was done so task "b" should have been allowed to
	// be added.
	popped, _ = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Check topic state
	topics = tracker.Topics()
	if len(topics.Active) != 1 {
		t.Fatal("Expected 1 active topics")
	}
	if len(topics.Pending) != 0 {
		t.Fatal("Expected no pending topics")
	}
}

type permissiveTaskMerger struct{}

func (*permissiveTaskMerger) HasNewInfo(task peertask.Task, existing []*peertask.Task) bool {
	return true
}
func (*permissiveTaskMerger) Merge(task peertask.Task, existing *peertask.Task) {
	existing.Data = task.Data
	existing.Work = task.Work
}

func TestReplaceTaskPermissive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 20,
			Work:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Push task "b". Has same topic and permissive task merger, so should
	// replace task "a".
	tracker.PushTasks(tasks[1]) // Topic "1"

	// Pop all tasks, should only be task "b".
	popped, _ := tracker.PopTasks(100)
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
	tracker := New(partner, &permissiveTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 10,
			Work:     20,
			Data:     "b",
		},
		{
			Topic:    "2",
			Priority: 5,
			Work:     5,
			Data:     "c",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Push task "b". Has same topic as task "a" and permissive task merger,
	// so should replace task "a", and update its Work from 10 to 20.
	tracker.PushTasks(tasks[1]) // Topic "1"

	// Push task "c"
	tracker.PushTasks(tasks[2]) // Topic "2"

	// Pop with target size 15. Should only pop task "a" because its Work
	// is now 20 (was 10)
	popped, pending := tracker.PopTasks(15)
	if len(popped) != 1 || popped[0].Data != "b" {
		t.Fatal("Expected 1 task")
	}
	if pending != 5 {
		t.Fatal("Expected pending work to be 5")
	}
	popped, pending = tracker.PopTasks(30)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}
}

func TestReplaceActiveTask(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 20,
			Work:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Pop task "a". This makes the task active.
	popped, _ := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	a := popped[0]

	// Push task "b"
	tracker.PushTasks(tasks[1]) // Topic "1"

	// Pop all tasks. Task "a" was active so task "b" should have been moved to
	// the pending queue.
	popped, _ = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	b := popped[0]

	// Finish tasks
	if tracker.IsIdle() {
		t.Error("expected an active task")
	}
	tracker.TaskDone(a)
	if tracker.IsIdle() {
		t.Error("expected an active task")
	}
	tracker.TaskDone(b)
	if !tracker.IsIdle() {
		t.Error("no active tasks")
	}
}

func TestReplaceActiveTaskNonPermissive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &DefaultTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 20,
			Work:     10,
			Data:     "b",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Pop task "a". This makes the task active.
	popped, _ := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b". Task merger is not permissive, so should ignore task "b".
	tracker.PushTasks(tasks[1]) // Topic "1"

	// Pop all tasks.
	popped, _ = tracker.PopTasks(100)
	if len(popped) != 0 {
		t.Fatal("Expected no tasks")
	}
}

func TestReplaceTaskThatIsActiveAndPending(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "b",
		},
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "c",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Pop task "a". This makes the task active.
	popped, _ := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b". Same Topic so should be added to the pending queue.
	tracker.PushTasks(tasks[1]) // Topic "1"

	// Push task "c". Permissive task merger so should replace pending task "b"
	// with same Topic.
	tracker.PushTasks(tasks[2]) // Topic "1"

	// Pop all tasks.
	popped, _ = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Data != "c" {
		t.Fatalf("Expected last task to overwrite pending task")
	}
}

func TestRemoveActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner, &permissiveTaskMerger{}, testMaxActiveWorkPerPeer)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     10,
			Data:     "a",
		},
		{
			Topic:    "1",
			Priority: 20,
			Work:     10,
			Data:     "b",
		},
		{
			Topic:    "2",
			Priority: 15,
			Work:     10,
			Data:     "c",
		},
	}

	// Push task "a"
	tracker.PushTasks(tasks[0]) // Topic "1"

	// Pop task "a". This makes the task active.
	popped, _ := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}

	// Push task "b" and "c"
	tracker.PushTasks(tasks[1]) // Topic "1"
	tracker.PushTasks(tasks[2]) // Topic "2"

	// Remove all tasks with Topic "1".
	// This should remove task "b" from the pending queue.
	tracker.Remove("1")
	popped, _ = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected tasks in order")
	}
}

func TestPushPopEqualTaskPriorities(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	clock := clock.NewMock()
	oldClock := clockInstance
	clockInstance = clock
	t.Cleanup(func() {
		clockInstance = oldClock
	})
	tracker := New(partner, &DefaultTaskMerger{}, 1)

	tasks := []peertask.Task{
		{
			Topic:    "1",
			Priority: 10,
			Work:     1,
		},
		{
			Topic:    "2",
			Priority: 10,
			Work:     1,
		},
		{
			Topic:    "3",
			Priority: 10,
			Work:     1,
		},
	}
	tracker.PushTasks(tasks[0])
	clock.Add(10 * time.Millisecond)
	tracker.PushTasks(tasks[1])
	clock.Add(10 * time.Millisecond)
	tracker.PushTasks(tasks[2])
	popped, _ := tracker.PopTasks(1)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "1" {
		t.Fatal("Expected first task")
	}
	tracker.TaskDone(popped[0])
	popped, _ = tracker.PopTasks(1)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "2" {
		t.Fatal("Expected second task")
	}
	tracker.TaskDone(popped[0])
	popped, _ = tracker.PopTasks(1)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Topic != "3" {
		t.Fatal("Expected third task")
	}
}
