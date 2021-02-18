package merkledag_test

import (
	"bytes"
	"context"
	"testing"

	. "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

func TestStableCID(t *testing.T) {
	nd := &MutableProtoNode{}
	nd.SetData([]byte("foobar"))
	nd.SetLinks([]*ipld.Link{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	})
	expected, err := cid.Decode("QmSN3WED2xPLbYvBbfvew2ZLtui8EbFYYcbfkpKH5jwG9C")
	if err != nil {
		t.Fatal(err)
	}
	fnd := finalize(t, nd)
	if !fnd.Cid().Equals(expected) {
		t.Fatalf("Got CID %s, expected CID %s", fnd.Cid(), expected)
	}
}

func TestRemoveLink(t *testing.T) {
	nd := &MutableProtoNode{}
	nd.SetLinks([]*ipld.Link{
		{Name: "a"},
		{Name: "b"},
		{Name: "a"},
		{Name: "a"},
		{Name: "c"},
		{Name: "a"},
	})

	err := nd.RemoveNodeLink("a")
	if err != nil {
		t.Fatal(err)
	}

	if len(nd.Links()) != 2 {
		t.Fatal("number of links incorrect")
	}

	if nd.Links()[0].Name != "b" {
		t.Fatal("link order wrong")
	}

	if nd.Links()[1].Name != "c" {
		t.Fatal("link order wrong")
	}

	// should fail
	err = nd.RemoveNodeLink("a")
	if err != ErrLinkNotFound {
		t.Fatal("should have failed to remove link")
	}

	// ensure nothing else got touched
	if len(nd.Links()) != 2 {
		t.Fatal("number of links incorrect")
	}

	if nd.Links()[0].Name != "b" {
		t.Fatal("link order wrong")
	}

	if nd.Links()[1].Name != "c" {
		t.Fatal("link order wrong")
	}
}

func TestFindLink(t *testing.T) {
	ctx := context.Background()

	ds := mdtest.Mock()
	ndEmpty := finalize(t, new(MutableProtoNode))
	err := ds.Add(ctx, ndEmpty)
	if err != nil {
		t.Fatal(err)
	}

	kEmpty := ndEmpty.Cid()

	nd := &MutableProtoNode{}
	nd.SetLinks([]*ipld.Link{
		{Name: "a", Cid: kEmpty},
		{Name: "c", Cid: kEmpty},
		{Name: "b", Cid: kEmpty},
	})

	fnd := finalize(t, nd)
	err = ds.Add(ctx, fnd)
	if err != nil {
		t.Fatal(err)
	}

	lnk, err := fnd.GetNodeLink("b")
	if err != nil {
		t.Fatal(err)
	}

	if lnk.Name != "b" {
		t.Fatal("got wrong link back")
	}

	_, err = fnd.GetNodeLink("f")
	if err != ErrLinkNotFound {
		t.Fatal("shouldnt have found link")
	}

	_, err = fnd.GetLinkedNode(context.Background(), ds, "b")
	if err != nil {
		t.Fatal(err)
	}

	outnd, err := nd.UpdateNodeLink("b", fnd)
	if err != nil {
		t.Fatal(err)
	}
	foutnd := finalize(t, outnd)

	olnk, err := foutnd.GetNodeLink("b")
	if err != nil {
		t.Fatal(err)
	}

	if olnk.Cid.String() == kEmpty.String() {
		t.Fatal("new link should have different hash")
	}
}

func TestNodeCopy(t *testing.T) {
	nd := &MutableProtoNode{}
	nd.SetLinks([]*ipld.Link{
		{Name: "a"},
		{Name: "c"},
		{Name: "b"},
	})

	nd.SetData([]byte("testing"))

	ond := nd.Copy()
	ond.SetData(nil)

	if nd.Data() == nil {
		t.Fatal("should be different objects")
	}
}

func TestJsonRoundtrip(t *testing.T) {
	nd := new(MutableProtoNode)
	nd.SetLinks([]*ipld.Link{
		{Name: "a"},
		{Name: "c"},
		{Name: "b"},
	})
	nd.SetData([]byte("testing"))
	fnd := finalize(t, nd)

	jb, err := fnd.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	nn := new(MutableProtoNode)
	err = nn.UnmarshalJSON(jb)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(nn.Data(), nd.Data()) {
		t.Fatal("data wasnt the same")
	}

	fnn := finalize(t, nn)
	if !fnn.Cid().Equals(fnd.Cid()) {
		t.Fatal("objects differed after marshaling")
	}
}
