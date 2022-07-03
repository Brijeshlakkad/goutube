package locus

import "github.com/hashicorp/raft"

type Config struct {
	Raft struct {
		raft.Config
		BindAdr string
		// StreamLayer *StreamLayer
		Bootstrap bool
	}
}
