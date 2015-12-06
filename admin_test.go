package hbase

import (
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
)

type AdminTestSuit struct {
	cli       HBaseClient
	tableName string
}

var (
	testZks = flag.String("zk", "localhost", "hbase zookeeper info")
)

func getTestZkHosts() []string {
	zks := strings.Split(*testZks, ",")
	if len(zks) == 0 {
		log.Fatal("invalid zk path")
	}
	return zks
}

var _ = Suite(&AdminTestSuit{})

func (s *AdminTestSuit) SetUpTest(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	s.tableName = "test_admin"
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS(s.tableName))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	s.cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
}

func (s *AdminTestSuit) TearDownTest(c *C) {
	err := s.cli.DisableTable(NewTableNameWithDefaultNS(s.tableName))
	c.Assert(err, IsNil)
	err = s.cli.DropTable(NewTableNameWithDefaultNS(s.tableName))
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TestTblExists(c *C) {
	cli, err := NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	b, err := cli.TableExists(s.tableName)
	c.Assert(b, IsTrue)
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TestCreateTableAsync(c *C) {
	cli, err := NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS(fmt.Sprintf("f_%d", i)))
			cf := NewColumnFamilyDescriptor("cf")
			tblDesc.AddColumnDesc(cf)
			cli.CreateTable(tblDesc, nil)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		tblName := fmt.Sprintf("f_%d", i)
		b, err := cli.TableExists(tblName)
		c.Assert(b, IsTrue)
		c.Assert(err, IsNil)

		tbl := NewTableNameWithDefaultNS(tblName)
		err = cli.DisableTable(tbl)
		if err != nil {
			log.Fatal(err)
		}
		err = cli.DropTable(tbl)
		if err != nil {
			log.Fatal(err)
		}
	}
}
