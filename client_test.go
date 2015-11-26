package hbase

import (
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
)

type ClientTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&ClientTestSuit{})

func (s *ClientTestSuit) SetUpTest(c *C) {
	s.cli, _ = NewClient(getTestZkHosts(), "/hbase")
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("xxx"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	s.cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
	log.Info("create table")
}

func (s *ClientTestSuit) TearDownTest(c *C) {
	s.cli.DisableTable(NewTableNameWithDefaultNS("xxx"))
	s.cli.DropTable(NewTableNameWithDefaultNS("xxx"))
}

func (s *ClientTestSuit) TestGetRegions(c *C) {
	regions := s.cli.GetRegions([]byte("xxx"), false)
	c.Assert(len(regions), Equals, 4)
}
