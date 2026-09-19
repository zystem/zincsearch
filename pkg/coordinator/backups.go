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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/coordinator/blob"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
)

// BackupStatus tells how far a stored backup has been checked.
type BackupStatus string

const (
	// BackupUploaded: stored and its checksum matched when it was uploaded.
	BackupUploaded BackupStatus = "uploaded"
	// BackupVerified: restored in a scratch node and its documents counted.
	BackupVerified BackupStatus = "verified"
	// BackupBad: damaged or unusable. It is never used to recover a node.
	BackupBad BackupStatus = "bad"
)

// BackupRecord is what the coordinator knows about a stored backup.
type BackupRecord struct {
	BackupInfo
	// Node is the node the backup was taken from.
	Node       string       `json:"node"`
	UploadedAt time.Time    `json:"uploaded_at"`
	Status     BackupStatus `json:"status"`
	// CheckedAt is when the stored object last matched its checksum.
	CheckedAt *time.Time `json:"checked_at,omitempty"`
	// VerifiedAt is when a scratch node last restored it successfully.
	VerifiedAt *time.Time `json:"verified_at,omitempty"`
	Note       string     `json:"note,omitempty"`

	revision uint64
}

const backupPrefix = "backups/"

// ErrNoBackupNode means there was no replica that could take a backup.
var ErrNoBackupNode = errors.New("coordinator: no healthy replica to take a backup from")

// ErrNoBackups means nothing usable is stored.
var ErrNoBackups = errors.New("coordinator: there is no usable backup")

// Schedule remembers when a periodic job ran, in the shared state, so a restart
// or a new leader neither repeats nor skips a run.
type Schedule struct {
	LastAttempt time.Time `json:"last_attempt"`
	LastSuccess time.Time `json:"last_success"`
	LastError   string    `json:"last_error,omitempty"`
}

func (c *Coordinator) loadSchedule(ctx context.Context, job string) (Schedule, uint64, error) {
	var s Schedule
	rev, err := state.GetJSON(ctx, c.st, "schedule/"+job, &s)
	if errors.Is(err, state.ErrNotFound) {
		return Schedule{}, 0, nil
	}
	return s, rev, err
}

func (c *Coordinator) saveSchedule(ctx context.Context, job string, s Schedule) {
	for i := 0; i < 3; i++ {
		_, rev, err := c.loadSchedule(ctx, job)
		if err == nil {
			if rev == 0 {
				_, err = state.CreateJSON(ctx, c.st, "schedule/"+job, s)
			} else {
				_, err = state.UpdateJSON(ctx, c.st, "schedule/"+job, s, rev)
			}
		}
		if err == nil || !errors.Is(err, state.ErrConflict) {
			if err != nil {
				log.Error().Err(err).Str("job", job).Msg("coordinator: save the schedule")
			}
			return
		}
	}
}

// Schedules returns when the periodic jobs ran last.
func (c *Coordinator) Schedules(ctx context.Context) (backup, verify Schedule) {
	backup, _, _ = c.loadSchedule(ctx, "backup")
	verify, _, _ = c.loadSchedule(ctx, "verify")
	return backup, verify
}

func (c *Coordinator) due(s Schedule, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	now := c.cfg.Now()
	// The retry delay follows a failed attempt only: the last attempt is newer
	// than the last success. After a success just the interval counts.
	if s.LastAttempt.After(s.LastSuccess) && now.Sub(s.LastAttempt) < c.cfg.BackupRetry {
		return false
	}
	return s.LastSuccess.IsZero() || now.Sub(s.LastSuccess) >= interval
}

// startDueJobs starts the backup and the verification when their time has come.
// They run in the background, so a long backup never delays failover.
func (c *Coordinator) startDueJobs(ctx context.Context) {
	if c.blobs == nil {
		return
	}
	if s, _, err := c.loadSchedule(ctx, "backup"); err == nil && c.due(s, c.cfg.BackupInterval) {
		c.runJob(ctx, "backup", func(ctx context.Context) error { _, err := c.BackupOnce(ctx); return err })
	}
	if s, _, err := c.loadSchedule(ctx, "verify"); err == nil && c.due(s, c.cfg.VerifyInterval) {
		c.runJob(ctx, "verify", c.VerifyOnce)
	}
}

func (c *Coordinator) runJob(ctx context.Context, name string, fn func(context.Context) error) {
	if !c.jobMu.TryLock() {
		return
	}
	go func() {
		defer c.jobMu.Unlock()
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Str("job", name).Msg("coordinator: job failed")
		}
	}()
}

// pickBackupNode chooses the node to back up: a replica that is up. The master
// serves reads and is never paused for a backup.
func (c *Coordinator) pickBackupNode() (*Node, error) {
	master := c.ClusterState().Master
	var best *Node
	var freshest uint64
	for _, s := range c.NodeStatuses() {
		if s.Name == master || s.State != NodeUp {
			continue
		}
		if best == nil || s.Stream.LastApplied > freshest {
			best, freshest = c.nodes[s.Name], s.Stream.LastApplied
		}
	}
	if best == nil {
		return nil, ErrNoBackupNode
	}
	return best, nil
}

// BackupOnce takes a backup from a replica, stores it and prunes old ones.
//
// The replica makes the archive (pausing itself while it does), the coordinator
// streams it into the blob store while computing its checksum, and only a copy
// whose checksum equals the one the node reported is kept.
func (c *Coordinator) BackupOnce(ctx context.Context) (rec *BackupRecord, err error) {
	if c.blobs == nil {
		return nil, errors.New("coordinator: no blob store configured")
	}
	started := c.cfg.Now()
	sched, _, _ := c.loadSchedule(ctx, "backup")
	sched.LastAttempt = started
	defer func() {
		if err != nil {
			sched.LastError = err.Error()
		} else {
			sched.LastSuccess, sched.LastError = c.cfg.Now(), ""
		}
		c.saveSchedule(context.WithoutCancel(ctx), "backup", sched)
	}()

	node, err := c.pickBackupNode()
	if err != nil {
		return nil, err
	}
	info, err := node.CreateBackup(ctx, c.cfg.DrainTimeout)
	if err != nil {
		return nil, fmt.Errorf("backup on %s: %w", node.cfg.Name, err)
	}
	// The archive on the node's disk is only a staging copy.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if delErr := node.DeleteBackup(cleanup, info.Name); delErr != nil {
			log.Warn().Err(delErr).Str("node", node.cfg.Name).Str("backup", info.Name).Msg("coordinator: delete the staging backup")
		}
	}()

	dl, err := node.DownloadBackup(ctx, info.Name)
	if err != nil {
		return nil, fmt.Errorf("download from %s: %w", node.cfg.Name, err)
	}
	defer dl.Close()
	if dl.SHA256 != "" && dl.SHA256 != info.SHA256 {
		return nil, fmt.Errorf("node %s announced checksum %s but serves %s", node.cfg.Name, info.SHA256, dl.SHA256)
	}
	h := sha256.New()
	if err := c.blobs.Put(ctx, info.Name, io.TeeReader(dl, h), info.Size); err != nil {
		return nil, fmt.Errorf("store %s: %w", info.Name, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != info.SHA256 {
		_ = c.blobs.Delete(context.WithoutCancel(ctx), info.Name)
		return nil, fmt.Errorf("backup %s changed on the way from %s: checksum %s, expected %s", info.Name, node.cfg.Name, got, info.SHA256)
	}

	now := c.cfg.Now()
	rec = &BackupRecord{BackupInfo: info, Node: node.cfg.Name, UploadedAt: now, Status: BackupUploaded, CheckedAt: &now}
	if _, err := state.CreateJSON(ctx, c.st, backupPrefix+info.Name, rec); err != nil {
		return nil, fmt.Errorf("record %s: %w", info.Name, err)
	}
	log.Info().Str("backup", info.Name).Str("node", node.cfg.Name).Int64("bytes", info.Size).Uint64("offset", info.LastApplied).Msg("coordinator: backup stored")

	if err := c.Prune(ctx); err != nil {
		log.Error().Err(err).Msg("coordinator: prune backups")
	}
	return rec, nil
}

func (c *Coordinator) listBackups(ctx context.Context) ([]BackupRecord, error) {
	return listBackupRecords(ctx, c.st)
}

// ListBackups returns the backups recorded in the state, newest first.
func ListBackups(ctx context.Context, st state.Store) ([]BackupRecord, error) {
	return listBackupRecords(ctx, st)
}

func listBackupRecords(ctx context.Context, st state.Store) ([]BackupRecord, error) {
	entries, err := st.List(ctx, backupPrefix)
	if err != nil {
		return nil, err
	}
	recs := make([]BackupRecord, 0, len(entries))
	for _, e := range entries {
		var r BackupRecord
		rev, err := state.GetJSON(ctx, st, e.Key, &r)
		if err != nil {
			if errors.Is(err, state.ErrNotFound) {
				continue
			}
			return nil, err
		}
		r.revision = rev
		recs = append(recs, r)
	}
	sort.Slice(recs, func(i, j int) bool {
		if !recs[i].CreatedAt.Equal(recs[j].CreatedAt) {
			return recs[i].CreatedAt.After(recs[j].CreatedAt)
		}
		return recs[i].Name > recs[j].Name
	})
	return recs, nil
}

// Backups returns the stored backups, newest first.
func (c *Coordinator) Backups(ctx context.Context) ([]BackupRecord, error) {
	return c.listBackups(ctx)
}

// Prune keeps the newest BackupRetention usable backups and the newest verified
// one, and removes the rest. Damaged backups are removed too, but only when a
// usable one exists, so there is never less to recover from than before.
func (c *Coordinator) Prune(ctx context.Context) error {
	recs, err := c.listBackups(ctx)
	if err != nil {
		return err
	}
	keep := map[string]bool{}
	usable := 0
	for _, r := range recs {
		if r.Status == BackupBad {
			continue
		}
		usable++
		if usable <= c.cfg.BackupRetention {
			keep[r.Name] = true
		}
	}
	c.warnAboutOrphans(ctx, recs)
	if usable == 0 {
		return nil
	}
	for _, r := range recs {
		if r.Status == BackupVerified {
			keep[r.Name] = true
			break // the newest verified one
		}
	}
	var errs []error
	for _, r := range recs {
		if keep[r.Name] {
			continue
		}
		if err := c.blobs.Delete(ctx, r.Name); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", r.Name, err))
			continue
		}
		if err := c.st.Delete(ctx, backupPrefix+r.Name); err != nil {
			errs = append(errs, fmt.Errorf("forget %s: %w", r.Name, err))
			continue
		}
		log.Info().Str("backup", r.Name).Str("status", string(r.Status)).Msg("coordinator: backup pruned")
	}
	return errors.Join(errs...)
}

// warnAboutOrphans logs the backups in the blob store that the state does not
// know, e.g. after a crash between the upload and the record. They are never
// deleted automatically: when the state was lost they may be the only copies.
func (c *Coordinator) warnAboutOrphans(ctx context.Context, recs []BackupRecord) {
	known := make(map[string]bool, len(recs))
	for _, r := range recs {
		known[r.Name] = true
	}
	infos, err := c.blobs.List(ctx, "zincsearch-")
	if err != nil {
		return
	}
	var orphans []string
	for _, in := range infos {
		if !known[in.Name] && strings.HasSuffix(in.Name, ".tgz") {
			orphans = append(orphans, in.Name)
		}
	}
	if len(orphans) > 0 {
		log.Warn().Strs("objects", orphans).Msg("coordinator: backups in the blob store that the state does not know; they are kept, remove them by hand if they are not needed")
	}
}

// LatestUsable returns the backup to recover a node from, see the function of the same name.
func (c *Coordinator) LatestUsable(ctx context.Context) (*BackupRecord, error) {
	return LatestUsable(ctx, c.st)
}

// LatestUsable returns the backup to recover a node from: the newest verified
// one, or when none was verified yet, the newest one that is not known to be bad.
func LatestUsable(ctx context.Context, st state.Store) (*BackupRecord, error) {
	recs, err := listBackupRecords(ctx, st)
	if err != nil {
		return nil, err
	}
	var fallback *BackupRecord
	for i := range recs {
		switch recs[i].Status {
		case BackupVerified:
			return &recs[i], nil
		case BackupUploaded:
			if fallback == nil {
				fallback = &recs[i]
			}
		}
	}
	if fallback == nil {
		return nil, ErrNoBackups
	}
	return fallback, nil
}

// FetchBackup copies a stored backup to w and checks it against the recorded
// checksum on the way. An empty name selects LatestUsable.
func (c *Coordinator) FetchBackup(ctx context.Context, name string, w io.Writer) (*BackupRecord, error) {
	return FetchBackup(ctx, c.st, c.blobs, name, w)
}

// FetchBackup copies a stored backup to w and checks it against the recorded
// checksum on the way. An empty name selects LatestUsable. It is what recovers a
// node: fetch the newest good backup, then run `zincsearch restore` on it.
func FetchBackup(ctx context.Context, st state.Store, blobs blob.Store, name string, w io.Writer) (*BackupRecord, error) {
	if blobs == nil {
		return nil, errors.New("coordinator: no blob store configured")
	}
	var rec *BackupRecord
	if name == "" {
		r, err := LatestUsable(ctx, st)
		if err != nil {
			return nil, err
		}
		rec = r
	} else {
		var r BackupRecord
		if _, err := state.GetJSON(ctx, st, backupPrefix+name, &r); err != nil {
			return nil, err
		}
		rec = &r
	}
	rc, err := blobs.Get(ctx, rec.Name)
	if errors.Is(err, blob.ErrNotFound) {
		return nil, fmt.Errorf("%w: stored backup %s is missing: %w", ErrBackupDamaged, rec.Name, err)
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, h), rc); err != nil {
		return nil, err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != rec.SHA256 {
		return nil, fmt.Errorf("%w: stored backup %s: checksum %s, expected %s", ErrBackupDamaged, rec.Name, got, rec.SHA256)
	}
	return rec, nil
}

// ErrBackupDamaged marks a failure that proves the stored backup itself is
// unusable (missing object, checksum mismatch, archive that does not restore).
// Every other error, such as an unreachable blob store, says nothing about the
// backup and must not condemn it.
var ErrBackupDamaged = errors.New("backup is damaged")

func (c *Coordinator) saveRecord(ctx context.Context, r *BackupRecord) error {
	rev, err := state.UpdateJSON(ctx, c.st, backupPrefix+r.Name, r, r.revision)
	if err == nil {
		r.revision = rev
	}
	return err
}

func (c *Coordinator) markBad(ctx context.Context, r *BackupRecord, why error) {
	r.Status, r.Note = BackupBad, why.Error()
	log.Error().Err(why).Str("backup", r.Name).Msg("coordinator: ALERT stored backup is bad")
	if err := c.saveRecord(ctx, r); err != nil {
		log.Error().Err(err).Str("backup", r.Name).Msg("coordinator: record that a backup is bad")
	}
}

// VerifyOnce checks the stored backups.
//
// Every one that is not known to be bad is read back from the blob store and its
// checksum compared with the recorded one, which finds storage that rots. The
// newest one is then, when a Verifier is configured, restored in a scratch node
// and its documents are counted, which finds an archive that is intact but
// useless. Backups that fail are marked bad and reported in the log.
func (c *Coordinator) VerifyOnce(ctx context.Context) (err error) {
	if c.blobs == nil {
		return errors.New("coordinator: no blob store configured")
	}
	sched, _, _ := c.loadSchedule(ctx, "verify")
	sched.LastAttempt = c.cfg.Now()
	defer func() {
		if err != nil {
			sched.LastError = err.Error()
		} else {
			sched.LastSuccess, sched.LastError = c.cfg.Now(), ""
		}
		c.saveSchedule(context.WithoutCancel(ctx), "verify", sched)
	}()

	recs, err := c.listBackups(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for i := range recs {
		r := &recs[i]
		if r.Status == BackupBad {
			continue
		}
		if err := c.checkStored(ctx, r); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, ErrBackupDamaged) {
				c.markBad(ctx, r, err)
			}
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, err))
			continue
		}
		now := c.cfg.Now()
		r.CheckedAt = &now
		if err := c.saveRecord(ctx, r); err != nil {
			errs = append(errs, err)
		}
	}
	if c.verifier != nil {
		for i := range recs {
			r := &recs[i]
			if r.Status == BackupBad {
				continue
			}
			if err := c.deepVerify(ctx, r); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(err, ErrBackupDamaged) {
					c.markBad(ctx, r, err)
				}
				errs = append(errs, fmt.Errorf("%s: %w", r.Name, err))
			}
			break // only the newest usable backup is restored, it is the one that counts
		}
	}
	return errors.Join(errs...)
}

// checkStored reads the object and compares its checksum.
func (c *Coordinator) checkStored(ctx context.Context, r *BackupRecord) error {
	rc, err := c.blobs.Get(ctx, r.Name)
	if errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("%w: the object is missing from the blob store", ErrBackupDamaged)
	}
	if err != nil {
		return err
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != r.SHA256 {
		return fmt.Errorf("%w: checksum %s does not match the recorded %s", ErrBackupDamaged, got, r.SHA256)
	}
	return nil
}

func (c *Coordinator) deepVerify(ctx context.Context, r *BackupRecord) error {
	tmp, err := os.CreateTemp("", "zinc-backup-*.tgz")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := c.FetchBackup(ctx, r.Name, tmp); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := c.verifier.Deep(ctx, tmp.Name()); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return fmt.Errorf("%w: restore in a scratch node failed: %w", ErrBackupDamaged, err)
		}
		return fmt.Errorf("restore in a scratch node: %w", err)
	}
	now := c.cfg.Now()
	r.Status, r.VerifiedAt, r.Note = BackupVerified, &now, ""
	return c.saveRecord(ctx, r)
}
