package hbase

import (
	"bytes"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/go-hbase/proto"
)

type HBaseDelTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&HBaseDelTestSuit{})

func (s *HBaseDelTestSuit) SetUpTest(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, Equals, nil)
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("t2"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	s.cli.CreateTable(tblDesc, nil)
}

func (s *HBaseDelTestSuit) TestDel(c *C) {
	d := NewDelete([]byte("hello"))
	d.AddFamily([]byte("cf"))
	d.AddFamily([]byte("cf1"))
	msg := d.ToProto()

	p, ok := msg.(*proto.MutationProto)
	c.Assert(ok, Equals, true)
	c.Assert(bytes.Compare(p.Row, []byte("hello")), Equals, 0)
	c.Assert(*p.MutateType, Equals, *proto.MutationProto_DELETE.Enum())

	cv := p.GetColumnValue()
	c.Assert(len(cv), Equals, 2)

	for _, v := range cv {
		c.Assert(len(v.QualifierValue), Equals, 1)
		c.Assert(*v.QualifierValue[0].DeleteType, Equals, *proto.MutationProto_DELETE_FAMILY.Enum())
	}

	d = NewDelete([]byte("hello"))
	d.AddStringColumn("cf\x00", "q")
	d.AddStringColumn("cf", "q")
	d.AddStringColumn("cf", "q")
	msg = d.ToProto()
	p, _ = msg.(*proto.MutationProto)
	cv = p.GetColumnValue()
	c.Assert(len(cv), Equals, 2)

	for _, v := range cv {
		c.Assert(len(v.QualifierValue), Equals, 1)
		c.Assert(*v.QualifierValue[0].DeleteType, Equals, *proto.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum())
	}
}

func (s *HBaseDelTestSuit) TestWithClient(c *C) {
	// create new
	p := NewPut([]byte("test"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("val"))
	s.cli.Put("t2", p)
	// check it

	g := NewGet([]byte("test"))
	g.AddStringFamily("cf")
	r, err := s.cli.Get("t2", g)
	c.Assert(err, Equals, nil)
	c.Assert(string(r.Columns["cf:q"].Value), Equals, "val")
	log.Info(string(r.Columns["cf:q"].Value))
	// delete it

	d := NewDelete([]byte("test"))
	d.AddColumn([]byte("cf"), []byte("q"))
	b, err := s.cli.Delete("t2", d)
	c.Assert(err, Equals, nil)
	c.Assert(b, Equals, true)

	// check it
	r, err = s.cli.Get("t2", g)
	c.Assert(err, Equals, nil)
	c.Assert(r == nil, Equals, true)
}
