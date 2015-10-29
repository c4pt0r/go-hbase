package hbase

import "sync"

type conn struct {
	mu sync.Mutex
}
