package dagutils

import (
	"context"
	"errors"
	"strings"

	bserv "github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	syncds "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"

	dag "github.com/ipfs/go-merkledag"
)

// Editor represents a ProtoNode tree editor and provides methods to
// modify it.
type Editor struct {
	root *dag.ProtoNode

	// tmp is a temporary in memory (for now) dagstore for all of the
	// intermediary nodes to be stored in
	tmp ipld.DAGService

	// src is the dagstore with *all* of the data on it, it is used to pull
	// nodes from for modification (nil is a valid value)
	src ipld.DAGService
}

// NewMemoryDagService returns a new, thread-safe in-memory DAGService.
func NewMemoryDagService() ipld.DAGService {
	// build mem-datastore for editor's intermediary nodes
	bs := bstore.NewBlockstore(syncds.MutexWrap(ds.NewMapDatastore()))
	bsrv := bserv.New(bs, offline.Exchange(bs))
	return dag.NewDAGService(bsrv)
}

// NewDagEditor returns an ProtoNode editor.
//
// * root is the node to be modified
// * source is the dagstore to pull nodes from (optional)
func NewDagEditor(root *dag.ProtoNode, source ipld.DAGService) *Editor {
	return &Editor{
		root: root,
		tmp:  NewMemoryDagService(),
		src:  source,
	}
}

// GetNode returns the a copy of the root node being edited.
func (e *Editor) GetNode() *dag.ProtoNode {
	return e.root.Copy().(*dag.ProtoNode)
}

// GetDagService returns the DAGService used by this editor.
func (e *Editor) GetDagService() ipld.DAGService {
	return e.tmp
}

func addLink(ctx context.Context, ds ipld.DAGService, orig *dag.ProtoNode, root *dag.MutableProtoNode, childname string, childnd ipld.Node) (*dag.ProtoNode, error) {
	if childname == "" {
		return nil, errors.New("cannot create link with no name")
	}

	// ensure that the node we are adding is in the dagservice
	err := ds.Add(ctx, childnd)
	if err != nil {
		return nil, err
	}

	if orig != nil {
		_ = ds.Remove(ctx, orig.Cid())
	}

	// ensure no link with that name already exists
	_ = root.RemoveNodeLink(childname) // ignore error, only option is ErrNotFound

	if err := root.AddNodeLink(childname, childnd); err != nil {
		return nil, err
	}

	nd, err := root.Finalize()
	if err != nil {
		return nil, err
	}
	if err := ds.Add(ctx, nd); err != nil {
		return nil, err
	}
	return nd, nil
}

// InsertNodeAtPath inserts a new node in the tree and replaces the current root with the new one.
func (e *Editor) InsertNodeAtPath(ctx context.Context, pth string, toinsert ipld.Node, create func() *dag.MutableProtoNode) error {
	splpath := strings.Split(pth, "/")
	nd, err := e.insertNodeAtPath(ctx, e.root, e.root.Mutable(), splpath, toinsert, create)
	if err != nil {
		return err
	}
	e.root = nd
	return nil
}

func (e *Editor) getLinkedChild(ctx context.Context, orig *dag.ProtoNode, field string, create func() *dag.MutableProtoNode) (*dag.ProtoNode, *dag.MutableProtoNode, error) {
	if orig == nil {
		return nil, create(), nil
	}
	nd, err := orig.GetLinkedProtoNode(ctx, e.tmp, field)
	if err != nil {
		// if 'create' is true, we create directories on the way down as needed
		if err == dag.ErrLinkNotFound && create != nil {
			return nil, create(), nil
		} else if err == ipld.ErrNotFound {
			// try finding it in our source dagstore
			nd, err = orig.GetLinkedProtoNode(ctx, e.src, field)
		}

		// if we receive an ErrNotFound, then our second 'GetLinkedNode' call
		// also fails, we want to error out
		if err != nil {
			return nil, nil, err
		}
	}
	return nd, nd.Mutable(), nil
}

func (e *Editor) insertNodeAtPath(ctx context.Context, orig *dag.ProtoNode, root *dag.MutableProtoNode, path []string, toinsert ipld.Node, create func() *dag.MutableProtoNode) (*dag.ProtoNode, error) {
	if len(path) == 1 {
		return addLink(ctx, e.tmp, orig, root, path[0], toinsert)
	}

	nd, mnd, err := e.getLinkedChild(ctx, orig, path[0], create)
	if err != nil {
		return nil, err
	}

	ndprime, err := e.insertNodeAtPath(ctx, nd, mnd, path[1:], toinsert, create)
	if err != nil {
		return nil, err
	}

	if orig != nil {
		_ = e.tmp.Remove(ctx, orig.Cid())
	}

	_ = root.RemoveNodeLink(path[0])
	err = root.AddNodeLink(path[0], ndprime)
	if err != nil {
		return nil, err
	}

	final, err := root.Finalize()
	if err != nil {
		return nil, err
	}
	err = e.tmp.Add(ctx, final)
	if err != nil {
		return nil, err
	}

	return final, nil
}

// RmLink removes the link with the given name and updates the root node of
// the editor.
func (e *Editor) RmLink(ctx context.Context, pth string) error {
	splpath := strings.Split(pth, "/")
	nd, err := e.rmLink(ctx, e.root, splpath)
	if err != nil {
		return err
	}
	e.root = nd
	return nil
}

func (e *Editor) withLinkRemoved(ctx context.Context, root *dag.ProtoNode, path []string) (*dag.MutableProtoNode, error) {
	if len(path) == 1 {
		mutable := root.Mutable()
		// base case, remove node in question
		err := mutable.RemoveNodeLink(path[0])
		if err != nil {
			return nil, err
		}
		return mutable, nil
	}

	// search for node in both tmp dagstore and source dagstore
	nd, err := root.GetLinkedProtoNode(ctx, e.tmp, path[0])
	if err == ipld.ErrNotFound {
		nd, err = root.GetLinkedProtoNode(ctx, e.src, path[0])
	}

	if err != nil {
		return nil, err
	}

	nnode, err := e.rmLink(ctx, nd, path[1:])
	if err != nil {
		return nil, err
	}

	_ = e.tmp.Remove(ctx, root.Cid())

	mutable := root.Mutable()
	_ = mutable.RemoveNodeLink(path[0])
	err = mutable.AddNodeLink(path[0], nnode)
	if err != nil {
		return nil, err
	}
	return mutable, nil
}

func (e *Editor) rmLink(ctx context.Context, root *dag.ProtoNode, path []string) (*dag.ProtoNode, error) {
	mutable, err := e.withLinkRemoved(ctx, root, path)
	if err != nil {
		return nil, err
	}

	final, err := mutable.Finalize()
	if err != nil {
		return nil, err
	}

	err = e.tmp.Add(ctx, final)
	if err != nil {
		return nil, err
	}

	return final, nil
}

// Finalize writes the new DAG to the given DAGService and returns the modified
// root node.
func (e *Editor) Finalize(ctx context.Context, ds ipld.DAGService) (*dag.ProtoNode, error) {
	nd := e.GetNode()
	err := copyDag(ctx, nd, e.tmp, ds)
	return nd, err
}

func copyDag(ctx context.Context, nd ipld.Node, from, to ipld.DAGService) error {
	// TODO(#4609): make this batch.
	err := to.Add(ctx, nd)
	if err != nil {
		return err
	}

	for _, lnk := range nd.Links() {
		child, err := lnk.GetNode(ctx, from)
		if err != nil {
			if err == ipld.ErrNotFound {
				// not found means we didnt modify it, and it should
				// already be in the target datastore
				continue
			}
			return err
		}

		err = copyDag(ctx, child, from, to)
		if err != nil {
			return err
		}
	}
	return nil
}
