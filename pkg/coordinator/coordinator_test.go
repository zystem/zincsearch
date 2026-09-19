/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/coordinator/blob"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
)

var ctx = context.Background()

// cluster is a coordinator with fake nodes a (master), b and optionally c.
type cluster struct {
	t     *testing.T
	c     *Coordinator
	clk   *clock
	nodes map[string]*fakeNode
	st    state.Store
	dir   string
	blobs blob.Store
}

func newCluster(t *testing.T, names ...string) *cluster {
	t.Helper()
	if len(names) == 0 {
		names = []string{"a", "b"}
	}
	dir := t.TempDir()
	blobs, err := blob.NewFS(filepath.Join(dir, "blobs"))
	require.NoError(t, err)
	cl := &cluster{t: t, clk: newClock(), nodes: map[string]*fakeNode{}, st: state.NewMem(), dir: dir, blobs: blobs}
	var cfgs []NodeConfig
	for _, n := range names {
		cl.nodes[n] = newFakeNode(t)
		cfgs = append(cfgs, cl.nodes[n].config(n))
	}
	cl.c = cl.coordinator("main", cfgs, nil)
	return cl
}

func (cl *cluster) coordinator(id string, nodes []NodeConfig, v Verifier) *Coordinator {
	c, err := New(Config{
		ID: id, Nodes: nodes, FailThreshold: 2, MaxLag: 50, PromoteCooldown: time.Minute,
		BackupInterval: time.Hour, BackupRetry: 10 * time.Minute, BackupRetention: 3,
		VerifyInterval: 24 * time.Hour, Now: cl.clk.now,
	}, Deps{State: cl.st, Blobs: cl.blobs, Verifier: v})
	require.NoError(cl.t, err)
	return c
}

func (cl *cluster) poll() {
	cl.c.PollOnce(ctx)
	cl.c.refreshCluster(ctx)
}

func TestClassify(t *testing.T) {
	cfg := Config{FailThreshold: 3, MaxLag: 100}
	seen := time.Now()
	up := StreamStatus{Connected: true, Lag: 5}
	tests := map[string]struct {
		s    NodeStatus
		want NodeState
	}{
		"never asked":       {NodeStatus{}, NodeUnknown},
		"healthy":           {NodeStatus{LastSeen: seen, Stream: up}, NodeUp},
		"lag at the limit":  {NodeStatus{LastSeen: seen, Stream: StreamStatus{Connected: true, Lag: 100}}, NodeUp},
		"lag beyond":        {NodeStatus{LastSeen: seen, Stream: StreamStatus{Connected: true, Lag: 101}}, NodeLagging},
		"stream lost":       {NodeStatus{LastSeen: seen, Stream: StreamStatus{Connected: false}}, NodeDegraded},
		"one failed poll":   {NodeStatus{LastSeen: seen, Failures: 1, Stream: up}, NodeSuspect},
		"failing":           {NodeStatus{LastSeen: seen, Failures: 3, Stream: up}, NodeDown},
		"stream gap":        {NodeStatus{LastSeen: seen, Stream: StreamStatus{Connected: true, LastError: "stream does not reach back to the node's offset: x"}}, NodeNeedsRestore},
		"offset past end":   {NodeStatus{LastSeen: seen, Stream: StreamStatus{Connected: true, LastError: "node offset is ahead of the stream: x"}}, NodeNeedsRestore},
		"other error shown": {NodeStatus{LastSeen: seen, Stream: StreamStatus{Connected: true, LastError: "apply message 5: disk full"}}, NodeUp},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) { assert.Equal(t, tt.want, classify(tt.s, cfg)) })
	}
}

func TestConfigValidation(t *testing.T) {
	_, err := New(Config{}, Deps{State: state.NewMem()})
	require.Error(t, err)
	_, err = New(Config{Nodes: []NodeConfig{{Name: "a", URL: "http://x"}, {Name: "a", URL: "http://y"}}}, Deps{State: state.NewMem()})
	require.ErrorContains(t, err, "twice")
	_, err = New(Config{Nodes: []NodeConfig{{Name: "bad name", URL: "http://x"}}}, Deps{State: state.NewMem()})
	require.Error(t, err)
	_, err = New(Config{Nodes: []NodeConfig{{Name: "a", URL: "http://x"}}}, Deps{})
	require.Error(t, err)
}

func TestNodesAreClassifiedFromTheirAnswers(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	for _, s := range cl.c.NodeStatuses() {
		assert.Equal(t, NodeUp, s.State, s.Name)
		assert.Equal(t, uint64(100), s.Stream.LastApplied)
	}

	// The first failed poll is only suspicious, the second one makes it down.
	cl.nodes["b"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	assert.Equal(t, NodeSuspect, cl.c.nodeStatus("b").State)
	cl.poll()
	assert.Equal(t, NodeDown, cl.c.nodeStatus("b").State)
	assert.NotEmpty(t, cl.c.nodeStatus("b").Error)

	// One good answer brings it back.
	cl.nodes["b"].set(func(f *fakeNode) { f.down = false; f.stream.Lag = 500 })
	cl.poll()
	assert.Equal(t, NodeLagging, cl.c.nodeStatus("b").State)
}

func TestTheInitialMasterIsTheFirstNode(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	master, ok := cl.c.Master()
	require.True(t, ok)
	assert.Equal(t, "a", master.Name)
	assert.Equal(t, uint64(1), cl.c.ClusterState().Epoch)
}

func TestNoPromotionWhileTheMasterIsAlive(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	promoted, err := cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Empty(t, promoted)

	// A master that lost the stream still answers reads, so it stays master.
	cl.nodes["a"].set(func(f *fakeNode) { f.stream.Connected = false })
	cl.poll()
	cl.clk.advance(time.Hour)
	promoted, err = cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Empty(t, promoted)
}

func TestFailoverPromotesTheReplicaWhenTheMasterIsDown(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	cl.clk.advance(time.Hour)

	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	promoted, err := cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Empty(t, promoted, "one failed poll is not enough")
	cl.poll()
	promoted, err = cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, "b", promoted)

	master, _ := cl.c.Master()
	assert.Equal(t, "b", master.Name)
	assert.Equal(t, uint64(2), cl.c.ClusterState().Epoch)
	history, err := cl.c.History(ctx)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, Promotion{Epoch: 2, From: "a", To: "b", At: cl.clk.now(), Reason: "master a is down"}, history[0])

	// The old master comes back as a replica; nothing flips back.
	cl.nodes["a"].set(func(f *fakeNode) { f.down = false })
	cl.clk.advance(time.Hour)
	cl.poll()
	promoted, err = cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Empty(t, promoted)
	master, _ = cl.c.Master()
	assert.Equal(t, "b", master.Name)
}

func TestFailoverPicksTheFreshestReplicaAndOnlyOneThatIsUp(t *testing.T) {
	cl := newCluster(t, "a", "b", "c", "d")
	cl.nodes["b"].set(func(f *fakeNode) { f.stream.LastApplied = 90 })
	cl.nodes["c"].set(func(f *fakeNode) { f.stream.LastApplied = 99 })
	cl.nodes["d"].set(func(f *fakeNode) { f.stream.LastApplied = 100; f.stream.Lag = 10_000 }) // freshest position, but far behind
	cl.poll()
	cl.clk.advance(time.Hour)
	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	cl.poll()

	promoted, err := cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, "c", promoted, "d is lagging, b is behind c")
}

func TestFailoverDoesNothingWhenNoReplicaCanTakeOver(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	cl.clk.advance(time.Hour)
	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	cl.nodes["b"].set(func(f *fakeNode) { f.stream.Lag = 5000 })
	cl.poll()
	cl.poll()
	promoted, err := cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Empty(t, promoted)
	master, _ := cl.c.Master()
	assert.Equal(t, "a", master.Name)
}

func TestFailoverWaitsOutTheCooldownAndAMasterThatNeedsARestoreCounts(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	// The state was just written (initial master): no promotion inside the cooldown.
	cl.nodes["a"].set(func(f *fakeNode) {
		f.stream.LastError = "stream does not reach back to the node's offset: needs 5, starts at 9"
	})
	cl.poll()
	promoted, err := cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Empty(t, promoted, "cooldown")

	cl.clk.advance(2 * time.Minute)
	promoted, err = cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, "b", promoted, "a master that cannot continue the stream is replaced")
}

func TestPromotionResumesAPausedNode(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	cl.clk.advance(time.Hour)
	cl.nodes["b"].set(func(f *fakeNode) { f.stream.Paused = true })
	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	cl.poll()
	promoted, err := cl.c.FailoverOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, "b", promoted)
	cl.nodes["b"].mu.Lock()
	defer cl.nodes["b"].mu.Unlock()
	assert.Equal(t, 1, cl.nodes["b"].resumeHits)
	assert.False(t, cl.nodes["b"].stream.Paused)
}

func TestTwoCoordinatorsNeverPromoteTwice(t *testing.T) {
	cl := newCluster(t)
	other := cl.coordinator("other", []NodeConfig{cl.nodes["a"].config("a"), cl.nodes["b"].config("b")}, nil)
	cl.poll()
	other.PollOnce(ctx)
	other.refreshCluster(ctx)
	cl.clk.advance(time.Hour)
	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	for i := 0; i < 2; i++ {
		cl.c.PollOnce(ctx)
		other.PollOnce(ctx)
	}

	var wg sync.WaitGroup
	results := make([]string, 2)
	for i, c := range []*Coordinator{cl.c, other} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], _ = c.FailoverOnce(ctx)
		}()
	}
	wg.Wait()

	promotions := 0
	for _, r := range results {
		if r != "" {
			promotions++
		}
	}
	assert.Equal(t, 1, promotions, "results %v", results)
	shared, _, err := cl.c.loadCluster(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), shared.Epoch, "the epoch moved once")
	assert.Equal(t, shared, cl.c.ClusterState(), "both coordinators know the decision")
	assert.Equal(t, shared, other.ClusterState())
	history, err := cl.c.History(ctx)
	require.NoError(t, err)
	assert.Len(t, history, 1)
}

func TestPromoteByHand(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	require.NoError(t, cl.c.Promote(ctx, "b"))
	master, _ := cl.c.Master()
	assert.Equal(t, "b", master.Name)
	require.NoError(t, cl.c.Promote(ctx, "b"), "already master")
	require.Error(t, cl.c.Promote(ctx, "nobody"))

	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	cl.poll()
	err := cl.c.Promote(ctx, "a")
	require.Error(t, err, "a node that is down cannot become master")
}

func TestAdmissionFollowsTheNodes(t *testing.T) {
	cl := newCluster(t)
	assert.False(t, cl.c.Admit().Allowed, "nothing is known before the first poll")
	cl.poll()
	assert.True(t, cl.c.Admit().Allowed)

	cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	cl.poll()
	assert.True(t, cl.c.Admit().Allowed, "the replica still serves")

	cl.nodes["b"].set(func(f *fakeNode) { f.down = true })
	cl.poll()
	cl.poll()
	adm := cl.c.Admit()
	assert.False(t, adm.Allowed)
	assert.Contains(t, adm.Reasons[0], "no ZincSearch node")
}

func TestLeaderElectionHasOneLeaderAndHandsOver(t *testing.T) {
	cl := newCluster(t)
	nodes := []NodeConfig{cl.nodes["a"].config("a"), cl.nodes["b"].config("b")}
	other := cl.coordinator("other", nodes, nil)

	cl.c.tryLease(ctx)
	other.tryLease(ctx)
	assert.True(t, cl.c.IsLeader())
	assert.False(t, other.IsLeader())

	// The leader vanishes without releasing; the lease runs out.
	cl.clk.advance(time.Minute)
	other.tryLease(ctx)
	assert.True(t, other.IsLeader())
	cl.c.tryLease(ctx)
	assert.False(t, cl.c.IsLeader(), "the old leader learns it lost the lease")
}

func TestBackupComesFromAReplicaIntoTheBlobStore(t *testing.T) {
	cl := newCluster(t)
	cl.poll()

	rec, err := cl.c.BackupOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, "b", rec.Node, "the master is never backed up")
	assert.Equal(t, BackupUploaded, rec.Status)
	assert.Equal(t, uint64(100), rec.LastApplied)

	rc, err := cl.blobs.Get(ctx, rec.Name)
	require.NoError(t, err)
	stored := new(bytes.Buffer)
	_, _ = stored.ReadFrom(rc)
	_ = rc.Close()
	assert.Equal(t, "archive number 1 of the fake node, offset 100", stored.String())

	assert.Zero(t, cl.nodes["b"].backupCount(), "the staging copy is removed from the node")
	assert.Zero(t, cl.nodes["a"].backupCount())
	recs, err := cl.c.Backups(ctx)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, rec.Name, recs[0].Name)

	sched, _ := cl.c.Schedules(ctx)
	assert.Equal(t, cl.clk.now(), sched.LastSuccess)
}

func TestBackupNeedsAHealthyReplica(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	cl.nodes["b"].set(func(f *fakeNode) { f.stream.Lag = 9999 })
	cl.poll()
	_, err := cl.c.BackupOnce(ctx)
	require.ErrorIs(t, err, ErrNoBackupNode)

	sched, _ := cl.c.Schedules(ctx)
	assert.NotEmpty(t, sched.LastError)
	assert.True(t, sched.LastSuccess.IsZero())
}

func TestBackupRefusesABodyThatDiffersFromItsChecksum(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	cl.nodes["b"].set(func(f *fakeNode) { f.tamper = true })

	_, err := cl.c.BackupOnce(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "changed on the way")
	infos, err := cl.blobs.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, infos, "a damaged copy is not kept")
	recs, _ := cl.c.Backups(ctx)
	assert.Empty(t, recs)
	assert.Zero(t, cl.nodes["b"].backupCount())
}

func TestBackupReportsAFailureOnTheNode(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	cl.nodes["b"].set(func(f *fakeNode) { f.backupErr = "the node is not quiescent" })
	_, err := cl.c.BackupOnce(ctx)
	require.Error(t, err)
	var ne *NodeError
	require.True(t, errors.As(err, &ne))
	assert.Equal(t, 409, ne.Status)
}

func TestRetentionKeepsTheNewestAndTheNewestVerified(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	var names []string
	for i := 0; i < 5; i++ {
		rec, err := cl.c.BackupOnce(ctx)
		require.NoError(t, err)
		names = append(names, rec.Name)
		cl.clk.advance(time.Hour)
		if i == 0 {
			// The oldest backup is the only one that was ever verified.
			recs, _ := cl.c.listBackups(ctx)
			now := cl.clk.now()
			recs[0].Status, recs[0].VerifiedAt = BackupVerified, &now
			require.NoError(t, cl.c.saveRecord(ctx, &recs[0]))
		}
	}
	recs, err := cl.c.Backups(ctx)
	require.NoError(t, err)
	var kept []string
	for _, r := range recs {
		kept = append(kept, r.Name)
	}
	assert.ElementsMatch(t, []string{names[0], names[2], names[3], names[4]}, kept, "3 newest, plus the newest verified one")
	infos, _ := cl.blobs.List(ctx, "")
	assert.Len(t, infos, 4, "the pruned objects are gone from the blob store too")
}

func TestVerificationFindsAStoredBackupThatRotted(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	first, err := cl.c.BackupOnce(ctx)
	require.NoError(t, err)
	cl.clk.advance(time.Hour)
	second, err := cl.c.BackupOnce(ctx)
	require.NoError(t, err)

	// Bits rot in the newer object.
	path := filepath.Join(cl.dir, "blobs", second.Name)
	require.NoError(t, os.WriteFile(path, []byte("rotten"), 0o600))

	err = cl.c.VerifyOnce(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), second.Name)

	recs, _ := cl.c.Backups(ctx)
	status := map[string]BackupStatus{}
	for _, r := range recs {
		status[r.Name] = r.Status
	}
	assert.Equal(t, BackupBad, status[second.Name])
	assert.Equal(t, BackupUploaded, status[first.Name])

	// A recovery uses the good one, and a damaged copy can never be fetched silently.
	usable, err := cl.c.LatestUsable(ctx)
	require.NoError(t, err)
	assert.Equal(t, first.Name, usable.Name)
	_, err = cl.c.FetchBackup(ctx, second.Name, new(bytes.Buffer))
	require.ErrorContains(t, err, "damaged")

	// A missing object is found as well.
	require.NoError(t, os.Remove(filepath.Join(cl.dir, "blobs", first.Name)))
	err = cl.c.VerifyOnce(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
	_, err = cl.c.LatestUsable(ctx)
	require.ErrorIs(t, err, ErrNoBackups)
}

// fakeVerifier stands in for the scratch node.
type fakeVerifier struct {
	mu    sync.Mutex
	calls []string
	fail  bool
	got   []byte
}

func (v *fakeVerifier) Deep(_ context.Context, path string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls = append(v.calls, path)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	v.got = data
	if v.fail {
		exit := exec.Command("sh", "-c", "exit 1").Run()
		return fmt.Errorf("%w: index logs holds 3 documents after the restore, the backup says 4", exit)
	}
	return nil
}

func TestDeepVerificationMarksTheNewestBackupVerifiedOrBad(t *testing.T) {
	cl := newCluster(t)
	v := &fakeVerifier{}
	c := cl.coordinator("main", []NodeConfig{cl.nodes["a"].config("a"), cl.nodes["b"].config("b")}, v)
	c.PollOnce(ctx)
	c.refreshCluster(ctx)

	old, err := c.BackupOnce(ctx)
	require.NoError(t, err)
	cl.clk.advance(time.Hour)
	newest, err := c.BackupOnce(ctx)
	require.NoError(t, err)

	require.NoError(t, c.VerifyOnce(ctx))
	require.Len(t, v.calls, 1, "only the newest backup is restored")
	assert.Equal(t, "archive number 2 of the fake node, offset 100", string(v.got), "the verifier got the stored bytes")
	recs, _ := c.Backups(ctx)
	byName := map[string]BackupRecord{}
	for _, r := range recs {
		byName[r.Name] = r
	}
	assert.Equal(t, BackupVerified, byName[newest.Name].Status)
	require.NotNil(t, byName[newest.Name].VerifiedAt)
	assert.Equal(t, BackupUploaded, byName[old.Name].Status)
	usable, err := c.LatestUsable(ctx)
	require.NoError(t, err)
	assert.Equal(t, newest.Name, usable.Name, "recovery prefers the verified backup")

	// Next time the scratch node cannot use it.
	v.mu.Lock()
	v.fail = true
	v.mu.Unlock()
	cl.clk.advance(24 * time.Hour)
	err = c.VerifyOnce(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "restore in a scratch node failed")
	recs, _ = c.Backups(ctx)
	for _, r := range recs {
		if r.Name == newest.Name {
			assert.Equal(t, BackupBad, r.Status)
			assert.Contains(t, r.Note, "holds 3 documents")
		}
	}
	usable, err = c.LatestUsable(ctx)
	require.NoError(t, err)
	assert.Equal(t, old.Name, usable.Name, "a bad backup is never used to recover")
}

func TestPruneNeverLeavesLessToRecoverFromThanBefore(t *testing.T) {
	cl := newCluster(t)
	cl.poll()
	var recs []*BackupRecord
	for i := 0; i < 3; i++ {
		r, err := cl.c.BackupOnce(ctx)
		require.NoError(t, err)
		recs = append(recs, r)
		cl.clk.advance(time.Hour)
	}
	all, _ := cl.c.listBackups(ctx)
	for i := range all {
		all[i].Status = BackupBad
		require.NoError(t, cl.c.saveRecord(ctx, &all[i]))
	}
	require.NoError(t, cl.c.Prune(ctx))
	left, _ := cl.c.Backups(ctx)
	assert.Len(t, left, 3, "nothing usable exists, so nothing is deleted")

	// Once a usable backup exists, the bad ones go.
	good, err := cl.c.BackupOnce(ctx)
	require.NoError(t, err)
	left, _ = cl.c.Backups(ctx)
	require.Len(t, left, 1)
	assert.Equal(t, good.Name, left[0].Name)
	_ = recs
}

func TestJobsRunWhenTheyAreDueAndNotBefore(t *testing.T) {
	cl := newCluster(t)
	cl.poll()

	s, _, err := cl.c.loadSchedule(ctx, "backup")
	require.NoError(t, err)
	assert.True(t, cl.c.due(s, time.Hour), "never ran")

	_, err = cl.c.BackupOnce(ctx)
	require.NoError(t, err)
	s, _, _ = cl.c.loadSchedule(ctx, "backup")
	assert.False(t, cl.c.due(s, time.Hour), "just ran")
	cl.clk.advance(59 * time.Minute)
	assert.False(t, cl.c.due(s, time.Hour))
	cl.clk.advance(2 * time.Minute)
	assert.True(t, cl.c.due(s, time.Hour), "an hour passed")

	// After a failure the next try waits for the retry delay, not a whole interval.
	cl.nodes["b"].set(func(f *fakeNode) { f.backupErr = "busy" })
	_, err = cl.c.BackupOnce(ctx)
	require.Error(t, err)
	s, _, _ = cl.c.loadSchedule(ctx, "backup")
	assert.False(t, cl.c.due(s, time.Hour), "retry delay not over")
	cl.clk.advance(11 * time.Minute)
	assert.True(t, cl.c.due(s, time.Hour), "retry delay over")
	assert.False(t, cl.c.due(s, -1), "a negative interval switches the job off")
}

// An interval that is shorter than the retry delay must still be honoured after
// a success: the retry delay is for failures only.
func TestAShortIntervalIsNotHeldBackByTheRetryDelayAfterASuccess(t *testing.T) {
	cl := newCluster(t) // the retry delay is 10 minutes
	cl.poll()
	_, err := cl.c.BackupOnce(ctx)
	require.NoError(t, err)
	s, _, _ := cl.c.loadSchedule(ctx, "backup")

	cl.clk.advance(10 * time.Second)
	assert.False(t, cl.c.due(s, 15*time.Second), "the interval has not passed")
	cl.clk.advance(10 * time.Second)
	assert.True(t, cl.c.due(s, 15*time.Second), "the interval has passed, and nothing failed")

	// After a failure the same interval waits for the retry delay.
	cl.nodes["b"].set(func(f *fakeNode) { f.backupErr = "busy" })
	_, err = cl.c.BackupOnce(ctx)
	require.Error(t, err)
	s, _, _ = cl.c.loadSchedule(ctx, "backup")
	cl.clk.advance(20 * time.Second)
	assert.False(t, cl.c.due(s, 15*time.Second), "a failed attempt waits for the retry delay")
	cl.clk.advance(10 * time.Minute)
	assert.True(t, cl.c.due(s, 15*time.Second))
}

func TestRunElectsPollsAndFailsOverByItself(t *testing.T) {
	a, b := newFakeNode(t), newFakeNode(t)
	st := state.NewMem()
	c, err := New(Config{
		ID: "run", Nodes: []NodeConfig{a.config("a"), b.config("b")},
		PollInterval: 20 * time.Millisecond, FailThreshold: 2, PromoteCooldown: time.Millisecond,
		BackupInterval: -1, VerifyInterval: -1, LeaseTTL: time.Second,
	}, Deps{State: st})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(runCtx) }()
	defer func() { cancel(); <-done }()

	require.Eventually(t, func() bool { return c.IsLeader() && c.Admit().Allowed }, 10*time.Second, 10*time.Millisecond)
	master, _ := c.Master()
	assert.Equal(t, "a", master.Name)

	a.set(func(f *fakeNode) { f.down = true })
	require.Eventually(t, func() bool {
		m, _ := c.Master()
		return m.Name == "b"
	}, 10*time.Second, 10*time.Millisecond, "the replica takes over without anybody calling FailoverOnce")

	cancel()
	<-done
	// Stopping gives the lease up at once.
	other := state.NewLease(st, "leader", "someone-else", time.Minute, nil)
	ok, err := other.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
}

// flakyBlobs fails reads while down is set, like an S3 503.
type flakyBlobs struct {
	blob.Store
	mu   sync.Mutex
	down bool
}

func (f *flakyBlobs) setDown(v bool) { f.mu.Lock(); f.down = v; f.mu.Unlock() }

func (f *flakyBlobs) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	f.mu.Lock()
	down := f.down
	f.mu.Unlock()
	if down {
		return nil, errors.New("503 slow down")
	}
	return f.Store.Get(ctx, name)
}

func TestVerificationDoesNotCondemnBackupsOnATransientStorageError(t *testing.T) {
	cl := newCluster(t)
	flaky := &flakyBlobs{Store: cl.blobs}
	cl.blobs = flaky
	v := &fakeVerifier{}
	c := cl.coordinator("main", []NodeConfig{cl.nodes["a"].config("a"), cl.nodes["b"].config("b")}, v)
	c.PollOnce(ctx)
	c.refreshCluster(ctx)
	rec, err := c.BackupOnce(ctx)
	require.NoError(t, err)

	flaky.setDown(true)
	require.Error(t, c.VerifyOnce(ctx))
	recs, _ := c.Backups(ctx)
	require.Len(t, recs, 1)
	assert.NotEqual(t, BackupBad, recs[0].Status, "an unreachable store proves nothing about the backup")

	// Prune must keep it as well.
	require.NoError(t, c.Prune(ctx))
	flaky.setDown(false)
	cl.clk.advance(24 * time.Hour)
	require.NoError(t, c.VerifyOnce(ctx))
	usable, err := c.LatestUsable(ctx)
	require.NoError(t, err)
	assert.Equal(t, rec.Name, usable.Name)
	assert.Equal(t, BackupVerified, usable.Status)
}

type brokenVerifier struct{}

func (brokenVerifier) Deep(context.Context, string) error {
	return errors.New("fork/exec: no such file")
}

func TestDeepVerifierThatCannotRunDoesNotMarkTheBackupBad(t *testing.T) {
	cl := newCluster(t)
	c := cl.coordinator("main", []NodeConfig{cl.nodes["a"].config("a"), cl.nodes["b"].config("b")}, brokenVerifier{})
	c.PollOnce(ctx)
	c.refreshCluster(ctx)
	_, err := c.BackupOnce(ctx)
	require.NoError(t, err)
	require.Error(t, c.VerifyOnce(ctx))
	recs, _ := c.Backups(ctx)
	require.Len(t, recs, 1)
	assert.NotEqual(t, BackupBad, recs[0].Status)
}
