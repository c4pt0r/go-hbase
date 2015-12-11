package hbase

import (
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
	c.Assert(err, IsNil)
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("t2"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	err = s.cli.CreateTable(tblDesc, nil)
	c.Assert(err, IsNil)
}

func (s *HBaseDelTestSuit) TearDownTest(c *C) {
	err := s.cli.DisableTable(NewTableNameWithDefaultNS("t2"))
	c.Assert(err, IsNil)

	err = s.cli.DropTable(NewTableNameWithDefaultNS("t2"))
	c.Assert(err, IsNil)
}

func (s *HBaseDelTestSuit) TestDel(c *C) {
	d := NewDelete([]byte("hello"))
	d.AddFamily([]byte("cf"))
	d.AddFamily([]byte("cf1"))
	msg := d.ToProto()

	p, ok := msg.(*proto.MutationProto)
	c.Assert(ok, IsTrue)
	c.Assert(string(p.Row), Equals, "hello")
	c.Assert(*p.MutateType, Equals, *proto.MutationProto_DELETE.Enum())

	cv := p.GetColumnValue()
	c.Assert(cv, HasLen, 2)

	for _, v := range cv {
		c.Assert(v.QualifierValue, HasLen, 1)
		c.Assert(*v.QualifierValue[0].DeleteType, Equals, *proto.MutationProto_DELETE_FAMILY.Enum())
	}

	d = NewDelete([]byte("hello"))
	d.AddStringColumn("cf\x00", "q")
	d.AddStringColumn("cf", "q")
	d.AddStringColumn("cf", "q")
	msg = d.ToProto()
	p, _ = msg.(*proto.MutationProto)
	cv = p.GetColumnValue()
	c.Assert(cv, HasLen, 2)

	for _, v := range cv {
		c.Assert(v.QualifierValue, HasLen, 1)
		c.Assert(*v.QualifierValue[0].DeleteType, Equals, *proto.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum())
	}
}

func (s *HBaseDelTestSuit) TestDelWithClient(c *C) {
	// Test put a new value.
	p := NewPut([]byte("test"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("val"))
	ok, err := s.cli.Put("t2", p)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	g := NewGet([]byte("test"))
	g.AddStringFamily("cf")
	r, err := s.cli.Get("t2", g)
	c.Assert(err, IsNil)
	c.Assert(string(r.Columns["cf:q"].Value), Equals, "val")

	// Test delte the value.
	d := NewDelete([]byte("test"))
	d.AddColumn([]byte("cf"), []byte("q"))
	b, err := s.cli.Delete("t2", d)
	c.Assert(err, IsNil)
	c.Assert(b, IsTrue)

	r, err = s.cli.Get("t2", g)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
}
