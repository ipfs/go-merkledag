package dagutils

import (
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"
	"github.com/stretchr/testify/require"
)

func TestMergeDiffs(t *testing.T) {
	node1 := dag.NodeWithData([]byte("one"))
	node2 := dag.NodeWithData([]byte("two"))
	node3 := dag.NodeWithData([]byte("three"))
	node4 := dag.NodeWithData([]byte("four"))

	changesA := []*Change{
		{Add, "one", cid.Cid{}, node1.Cid()},
		{Remove, "two", node2.Cid(), cid.Cid{}},
		{Mod, "three", node3.Cid(), node4.Cid()},
	}

	changesB := []*Change{
		{Mod, "two", node2.Cid(), node3.Cid()},
		{Add, "four", cid.Cid{}, node4.Cid()},
	}

	changes, conflicts := MergeDiffs(changesA, changesB)
	if len(changes) != 3 {
		t.Fatal("unexpected merge changes")
	}

	expect := []*Change{
		changesA[0],
		changesA[2],
		changesB[1],
	}

	for i, change := range changes {
		if change.Type != expect[i].Type {
			t.Error("unexpected diff change type")
		}

		if change.Path != expect[i].Path {
			t.Error("unexpected diff change path")
		}

		if change.Before != expect[i].Before {
			t.Error("unexpected diff change before")
		}

		if change.After != expect[i].After {
			t.Error("unexpected diff change before")
		}
	}

	if len(conflicts) != 1 {
		t.Fatal("unexpected merge conflicts")
	}

	if conflicts[0].A != changesA[1] {
		t.Error("unexpected merge conflict a")
	}

	if conflicts[0].B != changesB[0] {
		t.Error("unexpected merge conflict b")
	}
}

func TestExtractSortedGroupedLinks(t *testing.T) {
	n := &dag.ProtoNode{}
	child1 := dag.NodeWithData([]byte("1"))
	child2 := dag.NodeWithData([]byte("2"))
	child3 := dag.NodeWithData([]byte("3"))
	child4 := dag.NodeWithData([]byte("4"))
	// FIXME: Keep track of created child index and abstract.

	n.AddNodeLink("A", child1)
	n.AddNodeLink("B", child2)

	require.Equal(t, []*sameNameLinks{
		{name: "A", list: []*ipld.Link{n.Links()[0]}},
		{name: "B", list: []*ipld.Link{n.Links()[1]}},
	}, extractSortedGroupedLinks(n))

	n.AddNodeLink("A", child3)
	n.AddNodeLink("B", child4)

	require.Equal(t, []*sameNameLinks{
		// Links indexes set by trial and error as order depends on hash inside CID.
		{name: "A", list: []*ipld.Link{n.Links()[2], n.Links()[0]}},
		{name: "B", list: []*ipld.Link{n.Links()[1], n.Links()[3]}},
	}, extractSortedGroupedLinks(n))
}

func TestDiff(t *testing.T) {
	ctx := context.Background()
	ds := mdtest.Mock()
	dt := diffTester{t, ctx, ds}

	// FIXME: Keep track of created child index and abstract.
	child1 := dag.NodeWithData([]byte("1"))
	child2 := dag.NodeWithData([]byte("2"))
	child3 := dag.NodeWithData([]byte("3"))
	child4 := dag.NodeWithData([]byte("4"))
	require.NoError(t, ds.AddMany(ctx, []ipld.Node{child1, child2, child3, child4}))

	a := &dag.ProtoNode{}
	b := &dag.ProtoNode{}

	dt.expectDiff(a, a, []*Change{})
	dt.expectDiff(a, b, []*Change{})

	a.AddNodeLink("A", child1)
	dt.expectDiff(a, b, []*Change{
		{Remove, "A", child1.Cid(), cid.Cid{}},
	})
	dt.expectDiff(b, a, []*Change{
		{Add, "A", cid.Cid{}, child1.Cid()},
	})

	b.AddNodeLink("A", child1)
	dt.expectDiff(a, b, []*Change{})

	// Recursively diff inside B's links and detect node data as the only difference.
	a.AddNodeLink("B", child1)
	b.AddNodeLink("B", child2)
	dt.expectDiff(a, b, []*Change{
		{Mod, "B/<NODE-DATA>", child1.Cid(), child2.Cid()},
	})

	// More than one link per name, report everything as remove/add.
	b.AddNodeLink("B", child3)
	dt.expectDiff(a, b, []*Change{
		{Remove, "B", child1.Cid(), cid.Cid{}},
		{Add, "B", cid.Cid{}, child2.Cid()},
		{Add, "B", cid.Cid{}, child3.Cid()},
	})
}

type diffTester struct {
	t   *testing.T
	ctx context.Context
	ds  ipld.DAGService
}

func (dt *diffTester) expectDiff(a, b *dag.ProtoNode, expected []*Change) {
	changes, err := Diff(dt.ctx, dt.ds, a, b)
	require.NoError(dt.t, err)
	require.Equal(dt.t, expected, changes)
}
