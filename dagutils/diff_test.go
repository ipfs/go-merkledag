package dagutils

import (
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"
)

func TestMergeDiffs(t *testing.T) {
	node1 := finalize(t, dag.NodeWithData([]byte("one")))
	node2 := finalize(t, dag.NodeWithData([]byte("two")))
	node3 := finalize(t, dag.NodeWithData([]byte("three")))
	node4 := finalize(t, dag.NodeWithData([]byte("four")))

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
		changesB[1],
		changesA[0],
		changesA[2],
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

func TestDiff(t *testing.T) {
	ctx := context.Background()
	ds := mdtest.Mock()

	mrootA := &dag.MutableProtoNode{}
	mrootB := &dag.MutableProtoNode{}

	child1 := finalize(t, dag.NodeWithData([]byte("one")))
	child2 := finalize(t, dag.NodeWithData([]byte("two")))
	child3 := finalize(t, dag.NodeWithData([]byte("three")))
	child4 := finalize(t, dag.NodeWithData([]byte("four")))

	mrootA.AddNodeLink("one", child1)
	mrootA.AddNodeLink("two", child2)
	rootA := finalize(t, mrootA)

	mrootB.AddNodeLink("one", child3)
	mrootB.AddNodeLink("four", child4)
	rootB := finalize(t, mrootB)

	nodes := []ipld.Node{child1, child2, child3, child4, rootA, rootB}
	if err := ds.AddMany(ctx, nodes); err != nil {
		t.Fatal("failed to add nodes")
	}

	changes, err := Diff(ctx, ds, rootA, rootB)
	if err != nil {
		t.Fatal("unexpected diff error")
	}

	if len(changes) != 3 {
		t.Fatal("unexpected diff changes")
	}

	expect := []Change{
		{Mod, "one", child1.Cid(), child3.Cid()},
		{Remove, "two", child2.Cid(), cid.Cid{}},
		{Add, "four", cid.Cid{}, child4.Cid()},
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
}

func finalize(t *testing.T, a *dag.MutableProtoNode) *dag.ProtoNode {
	nd, err := a.Finalize()
	if err != nil {
		t.Error(t, err)
	}
	return nd
}
