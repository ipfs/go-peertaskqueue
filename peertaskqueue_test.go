package peertaskqueue

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestPushPop(t *testing.T) {
	ptq := New()
	partner := testutil.GeneratePeers(1)[0]
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")
	consonants := func() []string {
		var out []string
		for _, letter := range alphabet {
			skip := false
			for _, vowel := range vowels {
				if letter == vowel {
					skip = true
				}
			}
			if !skip {
				out = append(out, letter)
			}
		}
		return out
	}()
	sort.Strings(alphabet)
	sort.Strings(vowels)
	sort.Strings(consonants)

	// add a bunch of blocks. cancel some. drain the queue. the queue should only have the kept tasks

	for _, index := range rand.Perm(len(alphabet)) { // add blocks for all letters
		letter := alphabet[index]
		t.Log(letter)

		// add tasks out of order, but with in-order priority
		ptq.PushTasks(partner, peertask.Task{Topic: letter, Priority: math.MaxInt32 - index})
	}
	for _, consonant := range consonants {
		ptq.Remove(consonant, partner)
	}

	ptq.FullThaw()

	var out []string
	for {
		_, received, _ := ptq.PopTasks(100)
		if len(received) == 0 {
			break
		}

		for _, task := range received {
			out = append(out, task.Topic.(string))
		}
	}

	// Tasks popped should already be in correct order
	for i, expected := range vowels {
		if out[i] != expected {
			t.Fatal("received", out[i], "expected", expected)
		}
	}
}

func TestFreezeUnfreeze(t *testing.T) {
	ptq := New()
	peers := testutil.GeneratePeers(4)
	a := peers[0]
	b := peers[1]
	c := peers[2]
	d := peers[3]

	// Push 5 blocks to each peer
	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		ptq.PushTasks(a, peertask.Task{Topic: is, Work: 1})
		ptq.PushTasks(b, peertask.Task{Topic: is, Work: 1})
		ptq.PushTasks(c, peertask.Task{Topic: is, Work: 1})
		ptq.PushTasks(d, peertask.Task{Topic: is, Work: 1})
	}

	// now, pop off four tasks, there should be one from each
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

	ptq.Remove("1", b)

	// b should be frozen, causing it to get skipped in the rotation
	matchNTasks(t, ptq, 3, a.Pretty(), c.Pretty(), d.Pretty())

	ptq.ThawRound()

	matchNTasks(t, ptq, 1, b.Pretty())

	// remove none existent task
	ptq.Remove("-1", b)

	// b should not be frozen
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

}

func TestFreezeUnfreezeNoFreezingOption(t *testing.T) {
	ptq := New(IgnoreFreezing(true))
	peers := testutil.GeneratePeers(4)
	a := peers[0]
	b := peers[1]
	c := peers[2]
	d := peers[3]

	// Have each push some blocks

	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		ptq.PushTasks(a, peertask.Task{Topic: is, Work: 1})
		ptq.PushTasks(b, peertask.Task{Topic: is, Work: 1})
		ptq.PushTasks(c, peertask.Task{Topic: is, Work: 1})
		ptq.PushTasks(d, peertask.Task{Topic: is, Work: 1})
	}

	// now, pop off four tasks, there should be one from each
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

	ptq.Remove("1", b)

	// b should not be frozen, so it wont get skipped in the rotation
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())
}

// This test checks that ordering of peers is correct
func TestPeerOrder(t *testing.T) {
	ptq := New()
	peers := testutil.GeneratePeers(3)
	a := peers[0]
	b := peers[1]
	c := peers[2]

	ptq.PushTasks(a, peertask.Task{Topic: "1", Work: 3, Priority: 2})
	ptq.PushTasks(a, peertask.Task{Topic: "2", Work: 1, Priority: 1})

	ptq.PushTasks(b, peertask.Task{Topic: "3", Work: 1, Priority: 3})
	ptq.PushTasks(b, peertask.Task{Topic: "4", Work: 3, Priority: 2})
	ptq.PushTasks(b, peertask.Task{Topic: "5", Work: 1, Priority: 1})

	ptq.PushTasks(c, peertask.Task{Topic: "6", Work: 2, Priority: 2})
	ptq.PushTasks(c, peertask.Task{Topic: "7", Work: 2, Priority: 1})

	// All peers have nothing in their active queue, so equal chance of any
	// peer being chosen
	var ps []string
	var ids []string
	for i := 0; i < 3; i++ {
		p, tasks, _ := ptq.PopTasks(1)
		ps = append(ps, p.String())
		ids = append(ids, fmt.Sprint(tasks[0].Topic))
	}
	matchArrays(t, ps, []string{a.String(), b.String(), c.String()})
	matchArrays(t, ids, []string{"1", "3", "6"})

	// Active queues:
	// a: 3            Pending: [1]
	// b: 1            Pending: [3, 1]
	// c: 2            Pending: [2]
	// So next peer should be b (least work in active queue)
	p, tsk, pending := ptq.PopTasks(1)
	if len(tsk) != 1 || p != b || tsk[0].Topic != "4" {
		t.Fatal("Expected ID 4 from peer b")
	}
	if pending != 1 {
		t.Fatal("Expected pending work to be 1")
	}

	// Active queues:
	// a: 3            Pending: [1]
	// b: 1 + 3        Pending: [1]
	// c: 2            Pending: [2]
	// So next peer should be c (least work in active queue)
	p, tsk, pending = ptq.PopTasks(1)
	if len(tsk) != 1 || p != c || tsk[0].Topic != "7" {
		t.Fatal("Expected ID 7 from peer c")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}

	// Active queues:
	// a: 3            Pending: [1]
	// b: 1 + 3        Pending: [1]
	// c: 2 + 2
	// So next peer should be a (least work in active queue)
	p, tsk, pending = ptq.PopTasks(1)
	if len(tsk) != 1 || p != a || tsk[0].Topic != "2" {
		t.Fatal("Expected ID 2 from peer a")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}

	// Active queues:
	// a: 3 + 1
	// b: 1 + 3        Pending: [1]
	// c: 2 + 2
	// a & c have no more pending tasks, so next peer should be b
	p, tsk, pending = ptq.PopTasks(1)
	if len(tsk) != 1 || p != b || tsk[0].Topic != "5" {
		t.Fatal("Expected ID 5 from peer b")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}

	// Active queues:
	// a: 3 + 1
	// b: 1 + 3 + 1
	// c: 2 + 2
	// No more pending tasks, so next pop should return nothing
	_, tsk, pending = ptq.PopTasks(1)
	if len(tsk) != 0 {
		t.Fatal("Expected no more tasks")
	}
	if pending != 0 {
		t.Fatal("Expected pending work to be 0")
	}
}

func TestHooks(t *testing.T) {
	var peersAdded []string
	var peersRemoved []string
	onPeerAdded := func(p peer.ID) {
		peersAdded = append(peersAdded, p.Pretty())
	}
	onPeerRemoved := func(p peer.ID) {
		peersRemoved = append(peersRemoved, p.Pretty())
	}
	ptq := New(OnPeerAddedHook(onPeerAdded), OnPeerRemovedHook(onPeerRemoved))
	peers := testutil.GeneratePeers(2)
	a := peers[0]
	b := peers[1]
	ptq.PushTasks(a, peertask.Task{Topic: "1"})
	ptq.PushTasks(b, peertask.Task{Topic: "2"})
	expected := []string{a.Pretty(), b.Pretty()}
	sort.Strings(expected)
	sort.Strings(peersAdded)
	if len(peersAdded) != len(expected) {
		t.Fatal("Incorrect number of peers added")
	}
	for i, s := range peersAdded {
		if expected[i] != s {
			t.Fatal("unexpected peer", s, expected[i])
		}
	}

	p, task, _ := ptq.PopTasks(100)
	ptq.TasksDone(p, task...)
	p, task, _ = ptq.PopTasks(100)
	ptq.TasksDone(p, task...)
	ptq.PopTasks(100)
	ptq.PopTasks(100)

	sort.Strings(peersRemoved)
	if len(peersRemoved) != len(expected) {
		t.Fatal("Incorrect number of peers removed")
	}
	for i, s := range peersRemoved {
		if expected[i] != s {
			t.Fatal("unexpected peer", s, expected[i])
		}
	}
}

func TestCleaningUpQueues(t *testing.T) {
	ptq := New()

	peer := testutil.GeneratePeers(1)[0]
	var peerTasks []peertask.Task
	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		peerTasks = append(peerTasks, peertask.Task{Topic: is})
	}

	// push a block, pop a block, complete everything, should be removed
	ptq.PushTasks(peer, peerTasks...)
	p, task, _ := ptq.PopTasks(100)
	ptq.TasksDone(p, task...)
	_, task, _ = ptq.PopTasks(100)

	if len(task) != 0 || len(ptq.peerTrackers) > 0 || ptq.pQueue.Len() > 0 {
		t.Fatal("PeerTracker should have been removed because it's idle")
	}

	// push a block, remove each of its entries, should be removed
	ptq.PushTasks(peer, peerTasks...)
	for _, peerTask := range peerTasks {
		ptq.Remove(peerTask.Topic, peer)
	}
	_, task, _ = ptq.PopTasks(100)

	if len(task) != 0 || len(ptq.peerTrackers) > 0 || ptq.pQueue.Len() > 0 {
		t.Fatal("Partner should have been removed because it's idle")
	}
}

func matchNTasks(t *testing.T, ptq *PeerTaskQueue, n int, expected ...string) {
	var targets []string
	for i := 0; i < n; i++ {
		p, tsk, _ := ptq.PopTasks(1)
		if len(tsk) != 1 {
			t.Fatal("expected 1 task at a time")
		}
		targets = append(targets, p.Pretty())
	}

	matchArrays(t, expected, targets)
}

func matchArrays(t *testing.T, str1, str2 []string) {
	if len(str1) != len(str2) {
		t.Fatal("array lengths did not match", str1, str2)
	}

	sort.Strings(str1)
	sort.Strings(str2)

	t.Log(str1)
	t.Log(str2)
	for i, s := range str2 {
		if str1[i] != s {
			t.Fatal("unexpected peer", s, str1[i])
		}
	}
}
