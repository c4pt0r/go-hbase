package hbase

import (
	"github.com/ngaut/log"
	. "gopkg.in/check.v1"
)

type AdminTestSuit struct{}

var _ = Suite(&AdminTestSuit{})

func (s *AdminTestSuit) SetUpTest(c *C) {
	cli, _ := NewClient([]string{"localhost"}, "/hbase")
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("xxx"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
	log.Info("create table")
}

func (s *AdminTestSuit) TearDownTest(c *C) {
	cli, _ := NewClient([]string{"localhost"}, "/hbase")
	cli.DisableTable(NewTableNameWithDefaultNS("xxx"))
	cli.DropTable(NewTableNameWithDefaultNS("xxx"))
}

func (s *AdminTestSuit) TestTblExists(c *C) {
	cli, _ := NewClient([]string{"localhost"}, "/hbase")
	b := cli.TableExists("xxx")
	c.Assert(b, Equals, true)
}
