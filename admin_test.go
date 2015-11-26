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

type AdminTestSuit struct{}

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
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	// clean up last test if do not exit correctly
	if cli.TableExists("xxx") {
		s.TearDownTest(c)
	}
	for i := 0; i < 10; i++ {
		tmpTbl := fmt.Sprintf("f_%d", i)
		if cli.TableExists(tmpTbl) {
			s.TearDownTest(c)
		}
	}

	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("xxx"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
	log.Info("create table")
}

func (s *AdminTestSuit) TearDownTest(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	cli.DisableTable(NewTableNameWithDefaultNS("xxx"))
	cli.DropTable(NewTableNameWithDefaultNS("xxx"))
}

func (s *AdminTestSuit) TestTblExists(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	b := cli.TableExists("xxx")
	c.Assert(b, Equals, true)
}

func (s *AdminTestSuit) TestCreateTableAsync(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			curTbl := fmt.Sprintf("f_%d", i)
			curTN := NewTableNameWithDefaultNS(curTbl)
			if cli.TableExists(curTbl) {
				cli.DisableTable(curTN)
				cli.DropTable(curTN)
			}
			tblDesc := NewTableDesciptor(curTN)
			cf := NewColumnFamilyDescriptor("cf")
			tblDesc.AddColumnDesc(cf)

			err := cli.CreateTable(tblDesc, nil)
			if err != nil {
				log.Fatal(err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		tbl := NewTableNameWithDefaultNS(fmt.Sprintf("f_%d", i))
		cli.DisableTable(tbl)
		cli.DropTable(tbl)
	}
}

func (s *AdminTestSuit) TestGetRegions(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	regions := cli.GetRegions([]byte("xxx"), false)
	c.Assert(len(regions), Equals, 4)
}

func (s *AdminTestSuit) TestTableAutoSplit(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	regions := cli.GetRegions([]byte("xxx"), false)
	c.Assert(len(regions), Equals, 4)
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
	err := cli.Split("xxx", "")
	c.Assert(err, IsNil)

	// sleep wait Split finish
	time.Sleep(1000 * time.Millisecond)
	regions = cli.GetRegions([]byte("xxx"), false)
	c.Assert(len(regions), Equals, 7)
}

func (s *AdminTestSuit) TestTableSplit(c *C) {
	cli, _ := NewClient(getTestZkHosts(), "/hbase")
	regions := cli.GetRegions([]byte("xxx"), false)
	c.Assert(len(regions), Equals, 4)
	for i := 0; i < 100; i++ {
		p := NewPut([]byte(fmt.Sprintf("b_%d", i)))
		p.AddStringValue("cf", "c", fmt.Sprintf("bb_%d", i))
		cli.Put("xxx", p)
	}
	err := cli.Split("xxx", "b_2")
	c.Assert(err, IsNil)

	// sleep wait Split finish
	time.Sleep(500 * time.Millisecond)
	regions = cli.GetRegions([]byte("xxx"), false)
	c.Assert(len(regions), Equals, 5)
}
