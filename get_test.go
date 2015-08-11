package hbase

import (
	"bytes"

	"github.com/c4pt0r/go-hbase/proto"
	. "gopkg.in/check.v1"
)

type HBaseGetTestSuit struct{}

var _ = Suite(&HBaseGetTestSuit{})

func (s *HBaseGetTestSuit) TestGet(c *C) {
	g := NewGet([]byte("row"))
	g.AddFamily([]byte("cf"))
	g.AddColumn([]byte("cf"), []byte("c"))
	g.AddColumn([]byte("cf"), []byte("v"))
	g.AddFamily([]byte("cf1"))

	msg := g.ToProto()
	p, _ := msg.(*proto.Get)

	c.Assert(len(p.Column), Equals, 2)

	for _, col := range p.Column {
		if bytes.Compare([]byte("cf"), col.Family) == 0 {
			c.Assert(len(col.Qualifier), Equals, 2)
		} else {
			c.Assert(len(col.Qualifier), Equals, 0)
		}
	}
}
