package merkledag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	ipld "github.com/ipld/go-ipld-prime"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	mh "github.com/multiformats/go-multihash"
)

// Common errors
var (
	ErrNotProtobuf  = fmt.Errorf("expected protobuf dag node")
	ErrLinkNotFound = fmt.Errorf("no link by that name")
)

// ProtoNode represents a node in the IPFS Merkle DAG.
// nodes have opaque data and a set of navigable links.
type ProtoNode struct {
	blocks.Block
	dagpb.PBNode
}

// LinkSlice is a slice of format.Links
type LinkSlice []*format.Link

func (ls LinkSlice) Len() int           { return len(ls) }
func (ls LinkSlice) Swap(a, b int)      { ls[a], ls[b] = ls[b], ls[a] }
func (ls LinkSlice) Less(a, b int) bool { return ls[a].Name < ls[b].Name }

// GetNodeLink returns a copy of the link with the given name.
func (n *ProtoNode) GetNodeLink(name string) (*format.Link, error) {
	iter := n.PBNode.Links.Iterator()
	for !iter.Done() {
		_, next := iter.Next()
		if next.Name.Exists() && next.Name.Must().String() == name {
			c := cid.Undef
			if next.FieldHash().Exists() {
				c = next.FieldHash().Must().Link().(cidlink.Link).Cid
			}
			size := uint64(0)
			if next.FieldTsize().Exists() {
				size = uint64(next.FieldTsize().Must().Int())
			}
			return &format.Link{
				Name: next.FieldName().Must().String(),
				Size: size,
				Cid:  c,
			}, nil
		}
	}
	return nil, ErrLinkNotFound
}

// GetLinkedProtoNode returns a copy of the ProtoNode with the given name.
func (n *ProtoNode) GetLinkedProtoNode(ctx context.Context, ds format.DAGService, name string) (*ProtoNode, error) {
	nd, err := n.GetLinkedNode(ctx, ds, name)
	if err != nil {
		return nil, err
	}

	pbnd, ok := nd.(*ProtoNode)
	if !ok {
		return nil, ErrNotProtobuf
	}

	return pbnd, nil
}

// GetLinkedNode returns a copy of the IPLD Node with the given name.
func (n *ProtoNode) GetLinkedNode(ctx context.Context, ds format.DAGService, name string) (format.Node, error) {
	lnk, err := n.GetNodeLink(name)
	if err != nil {
		return nil, err
	}

	return lnk.GetNode(ctx, ds)
}

// Copy returns a copy of the node.
func (n *ProtoNode) Copy() format.Node {
	nb := dagpb.Type.PBNode.NewBuilder()
	_ = dagpb.PBDecoder(nb, bytes.NewBuffer(n.RawData()))
	nd := nb.Build()
	return &ProtoNode{n.Block, nd.(dagpb.PBNode)}
}

// Data returns the data stored by this node.
func (n *ProtoNode) Data() []byte {
	return n.FieldData().Bytes()
}

// Size returns the total size of the data addressed by node,
// including the total sizes of references.
func (n *ProtoNode) Size() (uint64, error) {
	s := uint64(len(n.RawData()))
	iter := n.PBNode.Links.Iterator()
	for !iter.Done() {
		_, next := iter.Next()
		s += uint64(next.FieldTsize().Must().Int())
	}
	return s, nil
}

// Stat returns statistics on the node.
func (n *ProtoNode) Stat() (*format.NodeStat, error) {
	cumSize, err := n.Size()
	if err != nil {
		return nil, err
	}

	return &format.NodeStat{
		Hash:           n.Cid().String(),
		NumLinks:       int(n.PBNode.Links.Length()),
		BlockSize:      len(n.RawData()),
		LinksSize:      len(n.RawData()) - len(n.Data()), // includes framing.
		DataSize:       len(n.Data()),
		CumulativeSize: int(cumSize),
	}, nil
}

// Loggable implements the ipfs/go-log.Loggable interface.
func (n *ProtoNode) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"node": n.String(),
	}
}

// MarshalJSON returns a JSON representation of the node.
func (n *ProtoNode) MarshalJSON() ([]byte, error) {
	out := map[string]interface{}{
		"data":  n.Data(),
		"links": n.Links(),
	}

	return json.Marshal(out)
}

// Multihash hashes the encoded data of this node.
func (n *ProtoNode) Multihash() mh.Multihash {
	return n.Cid().Hash()
}

// Links returns the node links.
func (n *ProtoNode) Links() []*format.Link {
	links := make([]*format.Link, 0, n.PBNode.Links.Length())
	iter := n.PBNode.Links.Iterator()
	for !iter.Done() {
		_, next := iter.Next()
		name := ""
		if next.FieldName().Exists() {
			name = next.FieldName().Must().String()
		}
		c := cid.Undef
		if next.FieldHash().Exists() {
			c = next.FieldHash().Must().Link().(cidlink.Link).Cid
		}
		size := uint64(0)
		if next.FieldTsize().Exists() {
			size = uint64(next.FieldTsize().Must().Int())
		}
		link := &format.Link{
			Name: name,
			Size: size,
			Cid:  c,
		}
		links = append(links, link)
	}
	return links
}

// Resolve is an alias for ResolveLink.
func (n *ProtoNode) Resolve(path []string) (interface{}, []string, error) {
	return n.ResolveLink(path)
}

// ResolveLink consumes the first element of the path and obtains the link
// corresponding to it from the node. It returns the link
// and the path without the consumed element.
func (n *ProtoNode) ResolveLink(path []string) (*format.Link, []string, error) {
	if len(path) == 0 {
		return nil, nil, fmt.Errorf("end of path, no more links to resolve")
	}

	lnk, err := n.GetNodeLink(path[0])
	if err != nil {
		return nil, nil, err
	}

	return lnk, path[1:], nil
}

// Tree returns the link names of the ProtoNode.
// ProtoNodes are only ever one path deep, so anything different than an empty
// string for p results in nothing. The depth parameter is ignored.
func (n *ProtoNode) Tree(p string, depth int) []string {
	if p != "" {
		return nil
	}

	out := make([]string, 0, int(n.PBNode.Links.Length()))
	iter := n.PBNode.Links.Iterator()
	for !iter.Done() {
		_, lnk := iter.Next()
		if lnk.Name.Exists() {
			out = append(out, lnk.Name.Must().String())
		} else {
			out = append(out, "")
		}
	}
	return out
}

func ProtoNodeConverter(b blocks.Block, nd ipld.Node) (legacy.UniversalNode, error) {
	pn, ok := nd.(dagpb.PBNode)
	if !ok {
		return nil, ErrNotProtobuf
	}
	return &ProtoNode{b, pn}, nil
}

var _ legacy.UniversalNode = &ProtoNode{}
