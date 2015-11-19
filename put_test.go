package hbase

import (
	"bytes"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/go-hbase/proto"
	"github.com/pingcap/tidb/util/codec"
)

type HBasePutTestSuit struct{}

var _ = Suite(&HBasePutTestSuit{})

func (s *HBasePutTestSuit) TestPut(c *C) {
	g := NewPut([]byte("row"))
	g.AddValue([]byte("cf"), []byte("q"), []byte("val"))
	msg := g.ToProto()
	p, _ := msg.(*proto.MutationProto)

	c.Assert(*p.MutateType, Equals, *proto.MutationProto_PUT.Enum())

	for _, col := range p.ColumnValue {
		for _, v := range col.QualifierValue {
			c.Assert(bytes.Compare([]byte("q"), v.Qualifier), Equals, 0)
			c.Assert(bytes.Compare([]byte("val"), v.Value), Equals, 0)
		}
	}
}

func (s *HBasePutTestSuit) TestGetPut(c *C) {
	log.Info(codec.EncodeKey(170))

	p := NewPut([]byte("1_\xff\xff"))
	p2 := NewPut([]byte("1_\xff\xfe"))
	p3 := NewPut([]byte("1_\xff\xee"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("!"))
	p2.AddValue([]byte("cf"), []byte("q"), []byte("!"))
	p3.AddValue([]byte("cf"), []byte("q"), []byte("!"))

	cli, err := NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, Equals, nil)

	cli.Put("t2", p)
	cli.Put("t2", p2)
	cli.Put("t2", p3)

	scan := NewScan([]byte("t2"), 100, cli)
	scan.StartRow = []byte("1_")
	for {
		r := scan.Next()
		if r == nil {
			break
		}
		log.Info(r.SortedColumns[0].Row)
	}

	cli.Delete("t2", NewDelete([]byte("1_\xff\xff")))
	cli.Delete("t2", NewDelete([]byte("1_\xff\xfe")))
	cli.Delete("t2", NewDelete([]byte("1_\xff\xee")))

}
