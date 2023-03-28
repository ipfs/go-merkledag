go-merkledag
==================

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![Coverage Status](https://codecov.io/gh/ipfs/go-merkledag/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-merkledag/branch/master)

> go-merkledag implements the 'DAGService' interface and adds two ipld node types, Protobuf and Raw 

## Status

❗ This repo is not actively maintained and it should ideally be deprecated.  
The version that is still used within Kubo lives in https://github.com/ipfs/boxo/tree/main/ipld/merkledag 

## Table of Contents

- [TODO](#todo)
- [Contribute](#contribute)
- [License](#license)

## TODO

- Pull out dag-pb stuff into go-ipld-pb
- Pull 'raw nodes' out into go-ipld-raw (maybe main one instead)
- Move most other logic to go-ipld
- Make dagservice constructor take a 'blockstore' to avoid the blockservice offline nonsense
- deprecate this package

## Contribute

PRs are welcome!

Small note: If editing the Readme, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © Juan Batiz-Benet
