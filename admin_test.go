package hbase

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

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
			curTbl := fmt.Sprintf("f_%d", i)
			curTN := NewTableNameWithDefaultNS(curTbl)
			b, err := cli.TableExists(curTbl)
			c.Assert(err, IsNil)
			if b {
				cli.DisableTable(curTN)
				cli.DropTable(curTN)
			}
			tblDesc := NewTableDesciptor(curTN)
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

func (s *AdminTestSuit) TestGetRegions(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	regions, err := cli.GetRegions([]byte("xxx"), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
}

func (s *AdminTestSuit) TestTableAutoSplit(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	regions, err := cli.GetRegions([]byte("xxx"), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
	for c := 'b'; c < 'f'; c++ {
		// b_0, b_1, ...
		// if use themis but insert few row, it may not be splited even invoke Split explict
		for i := 0; i < 10000; i++ {
			p := NewPut([]byte(fmt.Sprintf("%c_%d", c, i)))
			p.AddStringValue("cf", "c", fmt.Sprintf("%c%c_%d", c, c, i))
			cli.Put("xxx", p)
		}
	}
	// just split first 3 region
	err = cli.Split("xxx", "")
	c.Assert(err, IsNil)

	// sleep wait Split finish
	time.Sleep(1000 * time.Millisecond)
	regions, err = cli.GetRegions([]byte("xxx"), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 7)
}

func (s *AdminTestSuit) TestTableSplit(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	regions, err := cli.GetRegions([]byte("xxx"), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
	for i := 0; i < 100; i++ {
		p := NewPut([]byte(fmt.Sprintf("b_%d", i)))
		p.AddStringValue("cf", "c", fmt.Sprintf("bb_%d", i))
		cli.Put("xxx", p)
	}
	err = cli.Split("xxx", "b_2")
	c.Assert(err, IsNil)

	// sleep wait Split finish
	time.Sleep(500 * time.Millisecond)
	regions, err = cli.GetRegions([]byte("xxx"), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 5)
}
