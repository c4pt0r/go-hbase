package hbase

import (
	"bytes"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/go-hbase/proto"
)

type HBasePutTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&HBasePutTestSuit{})

func (s *HBasePutTestSuit) SetUpTest(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("t2"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	err = s.cli.CreateTable(tblDesc, nil)
	c.Assert(err, IsNil)
}

func (s *HBasePutTestSuit) TearDownTest(c *C) {
	err := s.cli.DisableTable(NewTableNameWithDefaultNS("t2"))
	c.Assert(err, IsNil)

	err = s.cli.DropTable(NewTableNameWithDefaultNS("t2"))
	c.Assert(err, IsNil)
}

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
	p := NewPut([]byte("1_\xff\xff"))
	p2 := NewPut([]byte("1_\xff\xfe"))
	p3 := NewPut([]byte("1_\xff\xee"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("!"))
	p2.AddValue([]byte("cf"), []byte("q"), []byte("!"))
	p3.AddValue([]byte("cf"), []byte("q"), []byte("!"))

	cli, err := NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, Equals, nil)

	ok, err := cli.Put("t2", p)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	ok, err = cli.Put("t2", p2)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	ok, err = cli.Put("t2", p3)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	scan := NewScan([]byte("t2"), 100, cli)
	scan.StartRow = []byte("1_")
	for {
		r := scan.Next()
		if r == nil {
			break
		}
		log.Info(r.SortedColumns[0].Row)
	}

	ok, err = cli.Delete("t2", NewDelete([]byte("1_\xff\xff")))
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	ok, err = cli.Delete("t2", NewDelete([]byte("1_\xff\xfe")))
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	ok, err = cli.Delete("t2", NewDelete([]byte("1_\xff\xee")))
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
}
