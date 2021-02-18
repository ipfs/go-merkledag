package merkledag

import (
	"encoding/json"
	"fmt"

	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

var v0CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  0,
}

var v1CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  1,
}

// V0CidPrefix returns a prefix for CIDv0
func V0CidPrefix() cid.Prefix { return v0CidPrefix }

// V1CidPrefix returns a prefix for CIDv1 with the default settings
func V1CidPrefix() cid.Prefix { return v1CidPrefix }

// PrefixForCidVersion returns the Protobuf prefix for a given CID version
func PrefixForCidVersion(version int) (cid.Prefix, error) {
	switch version {
	case 0:
		return v0CidPrefix, nil
	case 1:
		return v1CidPrefix, nil
	default:
		return cid.Prefix{}, fmt.Errorf("unknown CID version: %d", version)
	}
}

type MutableProtoNode struct {
	links   []*format.Link
	data    []byte
	builder cid.Builder
}

// CidBuilder returns the CID Builder for this ProtoNode, it is never nil
func (n *MutableProtoNode) CidBuilder() cid.Builder {
	if n.builder == nil {
		n.builder = v0CidPrefix
	}
	return n.builder
}

// SetCidBuilder sets the CID builder if it is non nil, if nil then it
// is reset to the default value
func (n *MutableProtoNode) SetCidBuilder(builder cid.Builder) {
	if builder == nil {
		n.builder = v0CidPrefix
	} else {
		n.builder = builder.WithCodec(cid.DagProtobuf)
	}
}

// NodeWithData builds a new Protonode with the given data.
func NodeWithData(d []byte) *MutableProtoNode {
	return &MutableProtoNode{data: d}
}

// AddNodeLink adds a link to another node.
func (n *MutableProtoNode) AddNodeLink(name string, that format.Node) error {
	lnk, err := format.MakeLink(that)
	if err != nil {
		return err
	}

	lnk.Name = name

	n.AddRawLink(name, lnk)

	return nil
}

// AddRawLink adds a copy of a link to this node
func (n *MutableProtoNode) AddRawLink(name string, l *format.Link) error {
	n.links = append(n.links, &format.Link{
		Name: name,
		Size: l.Size,
		Cid:  l.Cid,
	})

	return nil
}

// RemoveNodeLink removes a link on this node by the given name.
func (n *MutableProtoNode) RemoveNodeLink(name string) error {
	ref := n.links[:0]
	found := false

	for _, v := range n.links {
		if v.Name != name {
			ref = append(ref, v)
		} else {
			found = true
		}
	}

	if !found {
		return ErrLinkNotFound
	}

	n.links = ref

	return nil
}

func (n *MutableProtoNode) Copy() *MutableProtoNode {
	nnode := new(MutableProtoNode)
	if len(n.data) > 0 {
		nnode.data = make([]byte, len(n.data))
		copy(nnode.data, n.data)
	}

	if len(n.links) > 0 {
		nnode.links = make([]*format.Link, len(n.links))
		copy(nnode.links, n.links)
	}

	nnode.builder = n.builder

	return nnode
}

// Data returns the data stored by this node.
func (n *MutableProtoNode) Data() []byte {
	return n.data
}

// SetData stores data in this nodes.
func (n *MutableProtoNode) SetData(d []byte) {
	n.data = d
}

// UpdateNodeLink return a copy of the node with the link name set to point to
// that. If a link of the same name existed, it is removed.
func (n *MutableProtoNode) UpdateNodeLink(name string, that *ProtoNode) (*MutableProtoNode, error) {
	newnode := n.Copy()
	_ = newnode.RemoveNodeLink(name) // ignore error
	err := newnode.AddNodeLink(name, that)
	return newnode, err
}

// UnmarshalJSON reads the node fields from a JSON-encoded byte slice.
func (n *MutableProtoNode) UnmarshalJSON(b []byte) error {
	s := struct {
		Data  []byte         `json:"data"`
		Links []*format.Link `json:"links"`
	}{}

	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	n.data = s.Data
	n.links = s.Links
	return nil
}

// Links returns the node links.
func (n *MutableProtoNode) Links() []*format.Link {
	return n.links
}

// SetLinks replaces the node links with the given ones.
func (n *MutableProtoNode) SetLinks(links []*format.Link) {
	n.links = links
}
