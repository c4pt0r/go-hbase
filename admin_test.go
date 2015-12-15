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
	b, err := s.cli.TableExists(s.tableName)
	c.Assert(b, IsTrue)
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TestCreateTableAsync(c *C) {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			curTbl := fmt.Sprintf("f_%d", i)
			curTN := NewTableNameWithDefaultNS(curTbl)
			b, err := s.cli.TableExists(curTbl)
			c.Assert(err, IsNil)
			if b {
				s.cli.DisableTable(curTN)
				s.cli.DropTable(curTN)
			}
			tblDesc := NewTableDesciptor(curTN)
			cf := NewColumnFamilyDescriptor("cf")
			tblDesc.AddColumnDesc(cf)
			s.cli.CreateTable(tblDesc, nil)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		tblName := fmt.Sprintf("f_%d", i)
		b, err := s.cli.TableExists(tblName)
		c.Assert(b, IsTrue)
		c.Assert(err, IsNil)

		tbl := NewTableNameWithDefaultNS(tblName)
		err = s.cli.DisableTable(tbl)
		if err != nil {
			log.Fatal(err)
		}
		err = s.cli.DropTable(tbl)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (s *AdminTestSuit) TestGetRegions(c *C) {
	regions, err := s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
}

func (s *AdminTestSuit) TestTableAutoSplit(c *C) {
	regions, err := s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
	prefixLower := 'b'
	prefixUpper := 'f'
	for prefix := prefixLower; prefix < prefixUpper; prefix++ {
		// b_0, b_1, ...
		// If insert few row, it may not be splited even invoke Split explicitly.
		for i := 0; i < 10000; i++ {
			p := NewPut([]byte(fmt.Sprintf("%c_%d", prefix, i)))
			p.AddStringValue("cf", "c", fmt.Sprintf("%c%c_%d", prefix, prefix, i))
			b, err := s.cli.Put(s.tableName, p)
			c.Assert(err, IsNil)
			c.Assert(b, IsTrue)
		}
	}
	err = s.cli.Split(s.tableName, "")
	c.Assert(err, IsNil)
	// Sleep wait Split finish.
	time.Sleep(1 * time.Second)

	regions, err = s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	// After insert 10K data,
	// with Themis coprocessor, it will be split to 7 regions,
	// without themis, that number will be 6.
	// Split depends on coprocessor behavior and hbase conf,
	// so we just know it will split at least one region.
	c.Assert(len(regions), Greater, 4)

	// Check all data are still.
	scan := NewScan([]byte(s.tableName), 1000, s.cli)
	cnt := 0
	for prefix := prefixLower; prefix < prefixUpper; prefix++ {
		for i := 0; i < 10000; i++ {
			r := scan.Next()
			c.Assert(r, NotNil)
			cnt++
		}
	}
	c.Assert(cnt, Equals, int(prefixUpper-prefixLower)*10000)
}

func (s *AdminTestSuit) TestTableSplit(c *C) {
	regions, err := s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
	for i := 0; i < 100; i++ {
		p := NewPut([]byte(fmt.Sprintf("b_%d", i)))
		p.AddStringValue("cf", "c", fmt.Sprintf("bb_%d", i))
		b, err := s.cli.Put(s.tableName, p)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
	err = s.cli.Split(s.tableName, "b_2")
	c.Assert(err, IsNil)

	// Sleep wait Split finish.
	time.Sleep(500 * time.Millisecond)
	regions, err = s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 5)
}
