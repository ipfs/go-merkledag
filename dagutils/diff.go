package dagutils

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	dag "github.com/ipfs/go-merkledag"
)

// ChangeType denotes type of change in Change
type ChangeType int

// These constants define the changes that can be applied to a DAG.
const (
	Add ChangeType = iota
	Remove
	Mod
)

// Change represents a change to a DAG and contains a reference to the old and
// new CIDs.
type Change struct {
	Type   ChangeType
	Path   string
	Before cid.Cid
	After  cid.Cid
}

// String prints a human-friendly line about a change.
func (c *Change) String() string {
	switch c.Type {
	case Add:
		return fmt.Sprintf("Added %s at %s", c.After.String(), c.Path)
	case Remove:
		return fmt.Sprintf("Removed %s from %s", c.Before.String(), c.Path)
	case Mod:
		return fmt.Sprintf("Changed %s to %s at %s", c.Before.String(), c.After.String(), c.Path)
	default:
		panic("nope")
	}
}

// ApplyChange applies the requested changes to the given node in the given dag.
func ApplyChange(ctx context.Context, ds ipld.DAGService, nd *dag.ProtoNode, cs []*Change) (*dag.ProtoNode, error) {
	e := NewDagEditor(nd, ds)
	for _, c := range cs {
		switch c.Type {
		case Add:
			child, err := ds.Get(ctx, c.After)
			if err != nil {
				return nil, err
			}

			childpb, ok := child.(*dag.ProtoNode)
			if !ok {
				return nil, dag.ErrNotProtobuf
			}

			err = e.InsertNodeAtPath(ctx, c.Path, childpb, nil)
			if err != nil {
				return nil, err
			}

		case Remove:
			err := e.RmLink(ctx, c.Path)
			if err != nil {
				return nil, err
			}

		case Mod:
			err := e.RmLink(ctx, c.Path)
			if err != nil {
				return nil, err
			}
			child, err := ds.Get(ctx, c.After)
			if err != nil {
				return nil, err
			}

			childpb, ok := child.(*dag.ProtoNode)
			if !ok {
				return nil, dag.ErrNotProtobuf
			}

			err = e.InsertNodeAtPath(ctx, c.Path, childpb, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	return e.Finalize(ctx, ds)
}

// Diff returns a set of changes that transform node 'a' into node 'b'.
// It only traverses links in the following cases:
// 1. two node's links number are greater than 0.
// 2. both of two nodes are ProtoNode.
// 3. there is only one link under the name being traversed.
// Otherwise, it compares the cid and emits a Mod change object.
func Diff(ctx context.Context, ds ipld.DAGService, a, b *dag.ProtoNode) ([]*Change, error) {
	// Short-circuit the identity case to avoid inspecting links.
	if a.Cid() == b.Cid() {
		return []*Change{}, nil
	}

	var out []*Change
	if !bytes.Equal(a.Data(), b.Data()) {
		// FIXME: This is a very dirty way of reporting a difference in Data()
		//  but the current `Change` API doesn't support anything but links.
		out = append(out, &Change{Type: Mod, Path: "<NODE-DATA>", Before: a.Cid(), After: b.Cid()})
	}

	allGroupsA := extractSortedGroupedLinks(a)
	allGroupsB := extractSortedGroupedLinks(b)
	// FIXME: We don't need the `sameNameLinks` abstraction and the preprocessing
	//  of grouping links. We can just process them in one pass with smarter index
	//  range manipulation, but for a first pass this is more clear.

	groupIdxA := 0
	groupIdxB := 0
	for groupIdxA < len(allGroupsA) && groupIdxB < len(allGroupsB) {
		groupA := allGroupsA[groupIdxA]
		groupB := allGroupsB[groupIdxB]

		nameCmp := strings.Compare(groupA.name, groupB.name)
		// Name mismatch: advance the group that is lexicographically behind.
		if nameCmp < 0 {
			out = append(out, groupA.reportAsChange(Remove)...)
			groupIdxA++
			continue
		} else if nameCmp > 0 {
			out = append(out, groupB.reportAsChange(Add)...)
			groupIdxB++
			continue
		}

		// Name match. No matter how we process the groups we should advance both A/B.
		groupIdxA++
		groupIdxB++

		// First filter under this name link CIDs present in both groups.
		groupA, groupB = filterCIDMatches(groupA, groupB)

		if !groupA.singleLink() || !groupB.singleLink() {
			// More than one link under the same name, just report both groups
			// as remove/adds.
			out = append(out, groupA.reportAsChange(Remove)...)
			out = append(out, groupB.reportAsChange(Add)...)
			continue
		}

		// Single links with different CIDs, go deeper in the diff..
		linkA := groupA.list[0]
		linkB := groupB.list[0]
		sub, err := diffLinks(ctx, ds, linkA, linkB)
		if err != nil {
			return nil, err
		}

		for _, c := range sub {
			c.Path = path.Join(linkA.Name, c.Path)
		}
		out = append(out, sub...)
	}

	// We exhausted at least one group, report the other as missing.
	for _, groupA := range allGroupsA[groupIdxA:] {
		out = append(out, groupA.reportAsChange(Remove)...)
	}
	for _, groupB := range allGroupsB[groupIdxB:] {
		out = append(out, groupB.reportAsChange(Add)...)
	}

	return out, nil
}

// FIXME(BLOCKING): Implement.
func filterCIDMatches(a *sameNameLinks, b *sameNameLinks) (*sameNameLinks, *sameNameLinks) {
	return a, b
}

// Wrapper to Diff retrieving nodes from links.
func diffLinks(ctx context.Context, ds ipld.DAGService, a, b *ipld.Link) ([]*Change, error) {
	nodeA, err := a.GetNode(ctx, ds)
	if err != nil {
		return nil, err
	}

	// FIXME(BLOCKING): Support the raw node type since it's very common in UnixFS files.
	// FIXME: Consider reporting the node we can't probe just as a diff and not
	//  an error.
	pbNodeA, ok := nodeA.(*dag.ProtoNode)
	if !ok {
		// FIXME: We should print a more complete path here.
		return nil, fmt.Errorf("node %s is not ProtoNode format", nodeA.Cid())
	}

	nodeB, err := b.GetNode(ctx, ds)
	if err != nil {
		return nil, err
	}

	pbNodeB, ok := nodeB.(*dag.ProtoNode)
	if !ok {
		// FIXME: We should print a more complete path here.
		return nil, fmt.Errorf("node %s is not ProtoNode format", nodeB.Cid())
	}

	return Diff(ctx, ds, pbNodeA, pbNodeB)
}

// Links in a node that share the same name.
type sameNameLinks struct {
	name string
	list []*ipld.Link
}

// Whether there is only one link under this name.
func (snl *sameNameLinks) singleLink() bool {
	return len(snl.list) == 1
}

func (snl *sameNameLinks) reportAsChange(t ChangeType) []*Change {
	out := make([]*Change, 0, len(snl.list))
	for _, l := range snl.list {
		switch t {
		case Add:
			out = append(out, &Change{Type: t, Path: l.Name, After: l.Cid})
		case Remove:
			out = append(out, &Change{Type: t, Path: l.Name, Before: l.Cid})
		default:
			panic("only add/remove supported")
		}
	}
	return out
}

func extractSortedGroupedLinks(n *dag.ProtoNode) []*sameNameLinks {
	if len(n.Links()) == 0 {
		return nil
	}

	links := n.Copy().(*dag.ProtoNode).Links()
	// FIXME(BLOCKING): Do we care about changing link order in the original
	//  node? Maybe we can avoid the copy altogether and sort the original.
	sort.SliceStable(links, func(i, j int) bool {
		nameCmp := strings.Compare(links[i].Name, links[j].Name)
		if nameCmp != 0 {
			return nameCmp < 0
		}
		return strings.Compare(links[i].Cid.String(), links[j].Cid.String()) < 0
		// FIXME(BLOCKING): Is there a canonical way to sort CIDs? (Even
		//  if the order is meaningless, just to have a stable order).
	})

	// Group links by name.
	groupedByName := make([]*sameNameLinks, 0, len(links))
	var group *sameNameLinks
	for _, l := range links {
		if group == nil || l.Name != group.name {
			// New group of same-named links.
			group = &sameNameLinks{
				name: l.Name,
				list: []*ipld.Link{l},
			}
			groupedByName = append(groupedByName, group)
		} else {
			group.list = append(group.list, l)
		}
	}

	return groupedByName
}

// Conflict represents two incompatible changes and is returned by MergeDiffs().
type Conflict struct {
	A *Change
	B *Change
}

// MergeDiffs takes two slice of changes and adds them to a single slice.
// When a Change from b happens to the same path of an existing change in a,
// a conflict is created and b is not added to the merged slice.
// A slice of Conflicts is returned and contains pointers to the
// Changes involved (which share the same path).
func MergeDiffs(a, b []*Change) ([]*Change, []Conflict) {
	paths := make(map[string]*Change)
	for _, c := range b {
		paths[c.Path] = c
	}

	var changes []*Change
	var conflicts []Conflict

	// NOTE: we avoid iterating over maps here to ensure iteration order is determistic. We
	// include changes from a first, then b.
	for _, changeA := range a {
		if changeB, ok := paths[changeA.Path]; ok {
			conflicts = append(conflicts, Conflict{changeA, changeB})
		} else {
			changes = append(changes, changeA)
		}
		delete(paths, changeA.Path)
	}

	for _, c := range b {
		if _, ok := paths[c.Path]; ok {
			changes = append(changes, c)
		}
	}

	return changes, conflicts
}
