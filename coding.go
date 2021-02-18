package merkledag

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	pb "github.com/ipfs/go-merkledag/pb"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// Make sure the user doesn't upgrade this file.
// We need to check *here* as well as inside the `pb` package *just* in case the
// user replaces *all* go files in that package.
const _ = pb.DoNotUpgradeFileEverItWillChangeYourHashes

// for now, we use a PBNode intermediate thing.
// because native go objects are nice.

func (n *ProtoNode) Mutable() *MutableProtoNode {
	m := new(MutableProtoNode)
	m.data = n.Data()
	m.links = n.Links()
	m.builder = n.Cid().Prefix()
	return m
}

func (m *MutableProtoNode) Finalize() (*ProtoNode, error) {
	sort.Stable(LinkSlice(m.links)) // keep links sorted
	nb := dagpb.Type.PBNode.NewBuilder()
	err := fluent.Recover(func() {
		fb := fluent.WrapAssembler(nb)
		fb.CreateMap(-1, func(fmb fluent.MapAssembler) {
			fmb.AssembleEntry("Links").CreateList(int64(len(m.links)), func(flb fluent.ListAssembler) {
				for _, link := range m.links {
					flb.AssembleValue().CreateMap(-1, func(fmb fluent.MapAssembler) {
						if link.Cid.Defined() {
							hash, err := cid.Cast(link.Cid.Bytes())
							if err != nil {
								panic(fluent.Error{Err: fmt.Errorf("unmarshal failed. %v", err)})
							}
							fmb.AssembleEntry("Hash").AssignLink(cidlink.Link{Cid: hash})
						}
						fmb.AssembleEntry("Name").AssignString(link.Name)
						fmb.AssembleEntry("Tsize").AssignInt(int64(link.Size))
					})
				}
			})
			fmb.AssembleEntry("Data").AssignBytes(m.data)
		})
	})
	if err != nil {
		return nil, err
	}
	nd := nb.Build()
	newData := new(bytes.Buffer)
	err = dagpb.PBEncoder(nd, newData)
	if err != nil {
		return nil, err
	}
	raw := newData.Bytes()
	c, err := m.CidBuilder().Sum(raw)
	if err != nil {
		return nil, err
	}
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		return nil, err
	}
	return &ProtoNode{blk, nd.(dagpb.PBNode)}, nil
}

// GetPBNode converts *ProtoNode into it's protocol buffer variant.
// If you plan on mutating the data of the original node, it is recommended
// that you call ProtoNode.Copy() before calling ProtoNode.GetPBNode()
func (n *ProtoNode) GetPBNode() *pb.PBNode {
	pbn := &pb.PBNode{}
	links := n.Links()
	if len(links) > 0 {
		pbn.Links = make([]*pb.PBLink, len(links))
	}

	for i, l := range links {
		pbn.Links[i] = &pb.PBLink{}
		pbn.Links[i].Name = &l.Name
		pbn.Links[i].Tsize = &l.Size
		if l.Cid.Defined() {
			pbn.Links[i].Hash = l.Cid.Bytes()
		}
	}

	data := n.Data()
	if len(data) > 0 {
		pbn.Data = data
	}
	return pbn
}

type UnhashedProtoNode struct {
	encoded []byte
	node    dagpb.PBNode
}

func (upn *UnhashedProtoNode) ToProtoNode(builder cid.Builder) (*ProtoNode, error) {
	c, err := builder.Sum(upn.encoded)
	if err != nil {
		return nil, err
	}
	blk, err := blocks.NewBlockWithCid(upn.encoded, c)
	if err != nil {
		return nil, err
	}
	return &ProtoNode{blk, upn.node}, nil
}

// DecodeProtobuf decodes raw data and returns a new Node instance.
func DecodeProtobuf(encoded []byte) (*UnhashedProtoNode, error) {
	nb := dagpb.Type.PBNode.NewBuilder()
	err := dagpb.RawDecoder(nb, bytes.NewBuffer(encoded))
	if err != nil {
		return nil, err
	}
	nd := nb.Build()
	if err != nil {
		return nil, fmt.Errorf("incorrectly formatted merkledag node: %s", err)
	}
	return &UnhashedProtoNode{encoded, nd.(dagpb.PBNode)}, nil
}

// DecodeProtobufBlock is a block decoder for protobuf IPLD nodes conforming to
// node.DecodeBlockFunc
func DecodeProtobufBlock(b blocks.Block) (format.Node, error) {
	c := b.Cid()
	if c.Type() != cid.DagProtobuf {
		return nil, fmt.Errorf("this function can only decode protobuf nodes")
	}

	decnd, err := DecodeProtobuf(b.RawData())
	if err != nil {
		if strings.Contains(err.Error(), "Unmarshal failed") {
			return nil, fmt.Errorf("the block referred to by '%s' was not a valid merkledag node", c)
		}
		return nil, fmt.Errorf("failed to decode Protocol Buffers: %v", err)
	}

	return &ProtoNode{b, decnd.node}, nil
}

// Type assertion
var _ format.DecodeBlockFunc = DecodeProtobufBlock
