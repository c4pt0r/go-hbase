package hbase

import (
	"fmt"
	"sync"

	"github.com/ngaut/log"
	. "gopkg.in/check.v1"
)

type AdminTestSuit struct{}

var _ = Suite(&AdminTestSuit{})

func (s *AdminTestSuit) SetUpTest(c *C) {
	cli, _ := NewClient([]string{"zoo"}, "/hbase")
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("xxx"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
	log.Info("create table")
}

func (s *AdminTestSuit) TearDownTest(c *C) {
	cli, _ := NewClient([]string{"zoo"}, "/hbase")
	cli.DisableTable(NewTableNameWithDefaultNS("xxx"))
	cli.DropTable(NewTableNameWithDefaultNS("xxx"))
}

func (s *AdminTestSuit) TestTblExists(c *C) {
	cli, _ := NewClient([]string{"zoo"}, "/hbase")
	b := cli.TableExists("xxx")
	c.Assert(b, Equals, true)
}

func (s *AdminTestSuit) TestCreateTableAsync(c *C) {
	cli, _ := NewClient([]string{"zoo"}, "/hbase")
	wg := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS(fmt.Sprintf("f_%d", i)))
			cf := NewColumnFamilyDescriptor("cf")
			tblDesc.AddColumnDesc(cf)

			err := cli.CreateTable(tblDesc, nil)
			if err != nil {
				log.Fatal(err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 50; i++ {
		tbl := NewTableNameWithDefaultNS(fmt.Sprintf("f_%d", i))
		cli.DisableTable(tbl)
		cli.DropTable(tbl)
	}
}
