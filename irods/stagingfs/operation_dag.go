package stagingfs

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

type OperationState string

const (
	OperationQueued  OperationState = "QUEUED"
	OperationRunning OperationState = "RUNNING"
	OperationFailed  OperationState = "FAILED"
	OperationBlocked OperationState = "BLOCKED"
)

// StagingOperation is one persistent node in the synchronization DAG.
// Dependencies must complete before this node becomes runnable.
type StagingOperation struct {
	ID           string           `json:"id"`
	Metadata     *StagingMetadata `json:"metadata"`
	Dependencies []string         `json:"dependencies,omitempty"`
	State        OperationState   `json:"state"`
	Urgent       bool             `json:"urgent,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
}

// OperationDAG owns synchronization ordering independently of logical path
// metadata. It is protected by StagingStateManager.mu.
type OperationDAG struct {
	nodes map[string]*StagingOperation

	// dependents is the reverse of the Dependencies edges: it maps an
	// operation to the operations waiting on it. A sync completes one
	// operation at a time, and each completion has to drop the edges that
	// pointed at it. Finding them by walking every node costs the size of the
	// whole backlog per completed operation, which makes flushing a large
	// staging area quadratic, so the edges are indexed as they are created.
	dependents map[string]map[string]struct{}
}

func newOperationDAG() *OperationDAG {
	return &OperationDAG{
		nodes:      make(map[string]*StagingOperation),
		dependents: make(map[string]map[string]struct{}),
	}
}

// linkDependencies records op as a dependent of every operation it waits on.
func (dag *OperationDAG) linkDependencies(op *StagingOperation) {
	for _, dependencyID := range op.Dependencies {
		dag.linkDependency(op.ID, dependencyID)
	}
}

func (dag *OperationDAG) linkDependency(id string, dependencyID string) {
	dependents := dag.dependents[dependencyID]
	if dependents == nil {
		dependents = map[string]struct{}{}
		dag.dependents[dependencyID] = dependents
	}
	dependents[id] = struct{}{}
}

func (dag *OperationDAG) unlinkDependency(id string, dependencyID string) {
	dependents := dag.dependents[dependencyID]
	if dependents == nil {
		return
	}
	delete(dependents, id)
	if len(dependents) == 0 {
		delete(dag.dependents, dependencyID)
	}
}

func newOperationID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", errors.Wrap(err, "failed to generate operation ID")
	}
	return hex.EncodeToString(data), nil
}

func (dag *OperationDAG) add(meta *StagingMetadata, dependencies []string, urgent bool) (*StagingOperation, error) {
	id, err := newOperationID()
	if err != nil {
		return nil, err
	}
	copyMeta := *meta
	copyMeta.OperationID = id
	op := &StagingOperation{
		ID:           id,
		Metadata:     &copyMeta,
		Dependencies: uniqueOperationIDs(dependencies),
		State:        OperationQueued,
		Urgent:       urgent,
		CreatedAt:    time.Now(),
	}
	dag.nodes[id] = op
	dag.linkDependencies(op)
	return op, nil
}

func (dag *OperationDAG) restore(op *StagingOperation) {
	copyOp := *op
	copyMeta := *op.Metadata
	copyOp.Metadata = &copyMeta
	if copyOp.State == OperationRunning {
		copyOp.State = OperationQueued
	}
	dag.nodes[copyOp.ID] = &copyOp
	dag.linkDependencies(&copyOp)
}

func (dag *OperationDAG) get(id string) *StagingOperation {
	return dag.nodes[id]
}

func (dag *OperationDAG) remove(id string) {
	op := dag.nodes[id]
	delete(dag.nodes, id)

	if op != nil {
		for _, dependencyID := range op.Dependencies {
			dag.unlinkDependency(id, dependencyID)
		}
	}

	for dependentID := range dag.dependents[id] {
		if dependent := dag.nodes[dependentID]; dependent != nil {
			dependent.Dependencies = removeOperationID(dependent.Dependencies, id)
		}
	}
	delete(dag.dependents, id)
}

// dependentsOf returns the operations that list id as a dependency.
func (dag *OperationDAG) dependentsOf(id string) []string {
	dependents := make([]string, 0, len(dag.dependents[id]))
	for nodeID := range dag.dependents[id] {
		if _, exists := dag.nodes[nodeID]; exists {
			dependents = append(dependents, nodeID)
		}
	}
	return dependents
}

// reinsert undoes remove: it puts op back and restores the dependency edges
// that pointed at it, so a removal that could not be persisted leaves the DAG
// exactly as it was.
func (dag *OperationDAG) reinsert(op *StagingOperation, dependents []string) {
	dag.nodes[op.ID] = op
	dag.linkDependencies(op)
	for _, id := range dependents {
		dag.addDependency(id, op.ID)
	}
}

func (dag *OperationDAG) addDependency(id string, dependencyID string) {
	if id == "" || dependencyID == "" || id == dependencyID {
		return
	}
	if op := dag.nodes[id]; op != nil {
		op.Dependencies = uniqueOperationIDs(append(op.Dependencies, dependencyID))
		dag.linkDependency(id, dependencyID)
	}
}

func (dag *OperationDAG) markUrgent(id string) {
	if op := dag.nodes[id]; op != nil {
		op.Urgent = true
	}
}

func (dag *OperationDAG) rebasePath(id string, oldRoot string, newRoot string) {
	op := dag.nodes[id]
	if op == nil || !pathInSubtree(op.Metadata.Path, oldRoot) {
		return
	}
	op.Metadata.Path = newRoot + op.Metadata.Path[len(strings.TrimRight(oldRoot, "/")):]
	if op.Metadata.OldPath != "" && pathInSubtree(op.Metadata.OldPath, oldRoot) {
		op.Metadata.OldPath = newRoot + op.Metadata.OldPath[len(strings.TrimRight(oldRoot, "/")):]
	}
	op.Metadata.LastModifiedAt = time.Now()
}

func (dag *OperationDAG) ready(gracePeriod time.Duration, includeAll bool) []*StagingOperation {
	now := time.Now()
	ready := make([]*StagingOperation, 0)
	for _, op := range dag.nodes {
		if op.State == OperationRunning || op.State == OperationBlocked || len(op.Dependencies) != 0 {
			continue
		}
		if !includeAll && !op.Urgent && now.Sub(op.Metadata.LastModifiedAt) < gracePeriod {
			continue
		}
		copyOp := *op
		copyMeta := *op.Metadata
		copyOp.Metadata = &copyMeta
		copyOp.Dependencies = append([]string(nil), op.Dependencies...)
		ready = append(ready, &copyOp)
	}
	sort.SliceStable(ready, func(i, j int) bool {
		left := operationPriority(ready[i])
		right := operationPriority(ready[j])
		if left != right {
			return left < right
		}
		if ready[i].Metadata.Action == ActionRmdir && ready[j].Metadata.Action == ActionRmdir {
			leftDepth := strings.Count(strings.Trim(ready[i].Metadata.Path, "/"), "/")
			rightDepth := strings.Count(strings.Trim(ready[j].Metadata.Path, "/"), "/")
			if leftDepth != rightDepth {
				return leftDepth > rightDepth
			}
		}
		if !ready[i].CreatedAt.Equal(ready[j].CreatedAt) {
			return ready[i].CreatedAt.Before(ready[j].CreatedAt)
		}
		return ready[i].ID < ready[j].ID
	})
	return ready
}

func (dag *OperationDAG) validate() error {
	const (
		unvisited = iota
		visiting
		visited
	)
	states := make(map[string]int, len(dag.nodes))
	var visit func(string) error
	visit = func(id string) error {
		switch states[id] {
		case visiting:
			return errors.Newf("operation DAG contains a cycle at %s", id)
		case visited:
			return nil
		}
		op := dag.nodes[id]
		if op == nil {
			return errors.Newf("operation DAG references missing node %s", id)
		}
		states[id] = visiting
		for _, dependencyID := range op.Dependencies {
			if dag.nodes[dependencyID] == nil {
				return errors.Newf("operation %s references missing dependency %s", id, dependencyID)
			}
			if err := visit(dependencyID); err != nil {
				return err
			}
		}
		states[id] = visited
		return nil
	}
	for id := range dag.nodes {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func operationPriority(op *StagingOperation) int {
	if op.Urgent {
		switch op.Metadata.Action {
		case ActionDelete:
			return 0
		case ActionRename, ActionRenameDir:
			return 1
		case ActionRmdir:
			return 2
		default:
			return 3
		}
	}
	return 4
}

func uniqueOperationIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func removeOperationID(ids []string, target string) []string {
	result := ids[:0]
	for _, id := range ids {
		if id != target {
			result = append(result, id)
		}
	}
	return result
}
