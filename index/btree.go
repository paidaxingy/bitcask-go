package index

import "github.com/google/btree"

// BTree 索引，主要封装了 google 的 btree kv
// https://github.com/google/btree

type BTree struct {
	tree *btree.BTree
}
