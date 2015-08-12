package hbase

import . "gopkg.in/check.v1"

type AdminTestSuit struct{}

var _ = Suite(&AdminTestSuit{})

func (s *AdminTestSuit) TestTblExists(c *C) {
	cli, _ := NewClient([]string{"localhost"}, "/hbase")
	b := cli.TableExists("t1")
	c.Assert(b, Equals, true)
}

/*
func (s *AdminTestSuit) TestCreateTbl(c *C) {
	cli, _ := NewClient([]string{"localhost"}, "/hbase")
	tblDesc := NewTableDesciptor("", "xxx")
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)

	cli.CreateTable(tblDesc, nil)
}
*/
