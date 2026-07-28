package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type transactionJournal struct {
	Version       int        `json:"version"`
	Target        string     `json:"target"`
	Candidate     string     `json:"candidate"`
	Displaced     string     `json:"displaced,omitempty"`
	Backup        string     `json:"backup,omitempty"`
	Baseline      FileDigest `json:"baseline"`
	Staged        FileDigest `json:"staged"`
	BaselineMode  uint32     `json:"baselineMode"`
	BaselineOwned bool       `json:"baselineOwned"`
}

type pendingJournalState uint8

const (
	pendingJournalUnknown pendingJournalState = iota
	pendingJournalCommitted
	pendingJournalUncommitted
)

func (store *fileBackupStore) resolvePending(
	ctx context.Context,
	snapshot *fileSnapshot,
) error {
	raw, err := readRootBounded(ctx, snapshot.root, snapshot.journalName, 64<<10)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrRepairRequired
	}
	var journal transactionJournal
	if json.Unmarshal(raw, &journal) != nil ||
		!validateJournal(snapshot, journal) {
		return ErrRepairRequired
	}
	state, err := classifyPendingJournal(ctx, snapshot, journal)
	if err != nil || state == pendingJournalUnknown {
		return ErrRepairRequired
	}
	return cleanupPendingJournal(snapshot, journal)
}

func classifyPendingJournal(
	ctx context.Context,
	snapshot *fileSnapshot,
	journal transactionJournal,
) (pendingJournalState, error) {
	target, _, err := digestRootContext(ctx, snapshot.root, journal.Target)
	if err != nil {
		return pendingJournalUnknown, err
	}
	candidate, _, err := digestRootContext(ctx, snapshot.root, journal.Candidate)
	if err != nil {
		return pendingJournalUnknown, err
	}
	var backup FileDigest
	if journal.Backup != "" {
		backup, _, err = digestRootContext(ctx, snapshot.root, journal.Backup)
		if err != nil {
			return pendingJournalUnknown, err
		}
	}
	if journal.Baseline.Exists {
		if backup != journal.Baseline {
			return pendingJournalUnknown, nil
		}
		switch {
		case target == journal.Staged && candidate == journal.Baseline:
			return pendingJournalCommitted, nil
		case target == journal.Baseline && candidate == journal.Staged:
			return pendingJournalUncommitted, nil
		default:
			return pendingJournalUnknown, nil
		}
	}
	switch {
	case target == journal.Staged && !candidate.Exists:
		return pendingJournalCommitted, nil
	case !target.Exists && candidate == journal.Staged:
		return pendingJournalUncommitted, nil
	default:
		return pendingJournalUnknown, nil
	}
}

func cleanupPendingJournal(
	snapshot *fileSnapshot,
	journal transactionJournal,
) error {
	removed := make(map[string]struct{}, 3)
	for _, name := range []string{journal.Displaced, journal.Backup, journal.Candidate} {
		if name != "" {
			if _, duplicate := removed[name]; duplicate {
				continue
			}
			removed[name] = struct{}{}
			if err := snapshot.root.Remove(name); err != nil &&
				!errors.Is(err, os.ErrNotExist) {
				return ErrRepairRequired
			}
		}
	}
	if err := snapshot.root.Remove(snapshot.journalName); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return ErrRepairRequired
	}
	if err := snapshot.parent.Sync(); err != nil {
		return ErrRepairRequired
	}
	return nil
}

func validateJournal(snapshot *fileSnapshot, journal transactionJournal) bool {
	if snapshot == nil ||
		journal.Version != 1 ||
		journal.Target != snapshot.targetName ||
		filepath.Base(journal.Target) != journal.Target ||
		!journal.Staged.Exists ||
		journal.BaselineOwned != journal.Baseline.Exists ||
		!validRecoverySlot(journal.Candidate, ".candidate") {
		return false
	}
	if journal.Baseline.Exists {
		return journal.Displaced == journal.Candidate &&
			validRecoverySlot(journal.Backup, ".backup")
	}
	return journal.Displaced == "" && journal.Backup == ""
}

func validRecoverySlot(name, suffix string) bool {
	const prefix = ".mcp-config-"
	if filepath.Base(name) != name ||
		!strings.HasPrefix(name, prefix) ||
		!strings.HasSuffix(name, suffix) {
		return false
	}
	random := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if len(random) != 32 {
		return false
	}
	for _, character := range random {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func writeJournal(
	ctx context.Context,
	snapshot *fileSnapshot,
	journal transactionJournal,
) error {
	raw, err := json.Marshal(journal)
	if err != nil {
		return ErrRollbackFailed
	}
	file, err := snapshot.root.OpenFile(
		snapshot.journalName,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return ErrRepairRequired
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = snapshot.root.Remove(snapshot.journalName)
		}
	}()
	if err := copyBounded(ctx, file, bytesReader(raw), 64<<10); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return ErrRollbackFailed
	}
	if err := file.Close(); err != nil {
		return ErrRollbackFailed
	}
	if err := snapshot.parent.Sync(); err != nil {
		return ErrRollbackFailed
	}
	ok = true
	return nil
}
