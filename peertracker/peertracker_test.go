package peertracker

import (
	"testing"

	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/testutil"
)

func TestEmpty(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	if len(tracker.PopTasks(100)) != 0 {
		t.Fatal("Expected no tasks")
	}
}

func TestPushPop(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     1,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "1" {
		t.Fatal("Expected same task")
	}
}

func TestPushPopSizeAndOrder(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     20,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "3",
			Priority:     15,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(5)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}

	popped = tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "2" {
		t.Fatal("Expected tasks in order")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Identifier != "3" || popped[1].Identifier != "1" {
		t.Fatal("Expected tasks in order")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
}

func TestRemove(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     20,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "3",
			Priority:     15,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
	}
	tracker.PushTasks(tasks)
	tracker.Remove("2")
	popped := tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Identifier != "3" || popped[1].Identifier != "1" {
		t.Fatal("Expected tasks in order")
	}
}

func TestRemoveMulti(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "1",
			Priority:     20,
			IsWantBlock:  false,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     15,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
	}
	tracker.PushTasks(tasks)
	tracker.Remove("1")
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "2" {
		t.Fatal("Expected tasks in order")
	}
}

func TestRemoveActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "1",
			Priority:     20,
			IsWantBlock:  false,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     15,
			IsWantBlock:  true,
			IsDontHave:   false,
			SendDontHave: false,
			Size:         10,
		},
	}

	tracker.PushTasks(tasks)

	// Pop highest priority task, ie ID "1" want-have
	// This makes the task active
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "1" {
		t.Fatal("Expected tasks in order")
	}

	// Remove all tasks with ID "1"
	tracker.Remove("1")
	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "2" {
		t.Fatal("Expected tasks in order")
	}
}

func TestPushHaveVsBlock(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}

	runTestCase := func(tasks []peertask.Task, expIsWantBlock bool) {
		tracker := New(partner)
		tracker.PushTasks(tasks)
		popped := tracker.PopTasks(100)
		if len(popped) != 1 {
			t.Fatalf("Expected 1 task, received %d tasks", len(popped))
		}
		if popped[0].IsWantBlock != expIsWantBlock {
			t.Fatalf("Expected task.IsWantBlock to be %t, received %t", expIsWantBlock, popped[0].IsWantBlock)
		}
	}
	const wantBlockType = true
	const wantHaveType = false

	// should ignore second want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, wantHaveType)
	// should ignore second want-block
	runTestCase([]peertask.Task{wantBlock, wantBlock}, wantBlockType)
	// want-have does not overwrite want-block
	runTestCase([]peertask.Task{wantBlock, wantHave}, wantBlockType)
	// want-block overwrites want-have
	runTestCase([]peertask.Task{wantHave, wantBlock}, wantBlockType)
}

func TestPushSizeInfo(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         20,
	}
	wantBlockDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   true,
		SendDontHave: false,
		Size:         10,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         30,
	}
	wantHaveDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   true,
		SendDontHave: false,
		Size:         10,
	}

	runTestCase := func(tasks []peertask.Task, expSize int) {
		tracker := New(partner)
		tracker.PushTasks(tasks)
		popped := tracker.PopTasks(100)
		if len(popped) != 1 {
			t.Fatalf("Expected 1 task, received %d tasks", len(popped))
		}
		if popped[0].Size != expSize {
			t.Fatalf("Expected task.Size to be %d, received %d", expSize, popped[0].Size)
		}
	}

	// want-block with size should update existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlock}, wantBlock.Size)
	// want-block (DONT_HAVE) size should not update existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlockDontHave}, wantBlock.Size)
	// want-have with size should update existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHave}, wantHave.Size)
	// want-have (DONT_HAVE) should not update existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantHaveDontHave}, wantHave.Size)
}

func TestPushHaveVsBlockActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}

	runTestCase := func(tasks []peertask.Task, expCount int) {
		tracker := New(partner)
		var popped []peertask.Task
		for _, task := range tasks {
			// Push the task
			tracker.PushTasks([]peertask.Task{task})
			// Pop the task (which makes it active)
			popped = append(popped, tracker.PopTasks(10)...)
		}
		if len(popped) != expCount {
			t.Fatalf("Expected %d tasks, received %d tasks", expCount, len(popped))
		}
	}

	// should ignore second want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, 1)
	// should ignore second want-block
	runTestCase([]peertask.Task{wantBlock, wantBlock}, 1)
	// want-have does not overwrite want-block
	runTestCase([]peertask.Task{wantBlock, wantHave}, 1)
	// can't replace want-have with want-block because want-have is active
	runTestCase([]peertask.Task{wantHave, wantBlock}, 2)
}

func TestPushSizeInfoActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantBlockDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   true,
		SendDontHave: false,
		Size:         10,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantHaveDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   true,
		SendDontHave: false,
		Size:         10,
	}

	runTestCase := func(tasks []peertask.Task, expCount int) {
		tracker := New(partner)
		var popped []peertask.Task
		for _, task := range tasks {
			// Push the task
			tracker.PushTasks([]peertask.Task{task})
			// Pop the task (which makes it active)
			popped = append(popped, tracker.PopTasks(20)...)
		}
		if len(popped) != expCount {
			t.Fatalf("Expected %d tasks, received %d tasks", expCount, len(popped))
		}
	}

	// want-block with size should be added if there is existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlock}, 2)
	// want-block (DONT_HAVE) should not be added if there is existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlockDontHave}, 1)
	// want-block (DONT_HAVE) should not be added if there is existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlockDontHave}, 1)
	// want-have with size should be added if there is existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHave}, 2)
	// want-have (DONT_HAVE) should not be added if there is existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantHaveDontHave}, 1)
	// want-have (DONT_HAVE) should not be added if there is existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHaveDontHave}, 1)
}

func TestReplaceTaskThatIsActiveAndPending(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantHaveDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   true,
		SendDontHave: false,
		Size:         10,
	}

	tracker := New(partner)

	// Push a want-have (DONT_HAVE)
	tracker.PushTasks([]peertask.Task{wantHaveDontHave})

	// Pop the want-have (DONT_HAVE) (which makes it active)
	popped := tracker.PopTasks(20)

	// Push a second want-have (with a size). Should be added to the pending
	// queue.
	tracker.PushTasks([]peertask.Task{wantHave})

	// Push a want-block (should replace the pending want-have)
	tracker.PushTasks([]peertask.Task{wantBlock})

	popped = tracker.PopTasks(20)
	if len(popped) != 1 {
		t.Fatalf("Expected 1 task to be popped, received %d tasks", len(popped))
	}
	if !popped[0].IsWantBlock {
		t.Fatalf("Expected task to be want-block")
	}
}

func TestTaskDone(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}
	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		IsDontHave:   false,
		SendDontHave: false,
		Size:         10,
	}

	runTestCase := func(tasks []peertask.Task, expCount int) {
		tracker := New(partner)
		var popped []peertask.Task
		for _, task := range tasks {
			// Push the task
			tracker.PushTasks([]peertask.Task{task})
			// Pop the task (which makes it active)
			poppedTask := tracker.PopTasks(10)
			if len(poppedTask) > 0 {
				popped = append(popped, poppedTask...)
				// Complete the task (which makes it inactive)
				tracker.TaskDone(poppedTask[0].Identifier, poppedTask[0].IsWantBlock)
			}
		}
		if len(popped) != expCount {
			t.Fatalf("Expected %d tasks, received %d tasks", expCount, len(popped))
		}
	}

	// should allow second want-have after first is complete
	runTestCase([]peertask.Task{wantHave, wantHave}, 2)
	// should allow second want-block after first is complete
	runTestCase([]peertask.Task{wantBlock, wantBlock}, 2)
	// should allow want-have after want-block is complete
	runTestCase([]peertask.Task{wantBlock, wantHave}, 2)
	// should allow want-block after want-have is complete
	runTestCase([]peertask.Task{wantHave, wantBlock}, 2)
}
