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
	cli              HBaseClient
	tableName        string
	invalidTableName string
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

func (s *AdminTestSuit) SetUpSuite(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	s.tableName = "test_admin"
	s.invalidTableName = "test_admin_xxx"
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS(s.tableName))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	err = s.cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TearDownSuite(c *C) {
	err := s.cli.DisableTable(NewTableNameWithDefaultNS(s.tableName))
	c.Assert(err, IsNil)
	err = s.cli.DropTable(NewTableNameWithDefaultNS(s.tableName))
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TestTableExists(c *C) {
	b, err := s.cli.TableExists(s.tableName)
	c.Assert(err, IsNil)
	c.Assert(b, IsTrue)

	b, err = s.cli.TableExists(s.invalidTableName)
	c.Assert(err, IsNil)
	c.Assert(b, IsFalse)
}

func (s *AdminTestSuit) TestCreateTableAsync(c *C) {
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			tblName := fmt.Sprintf("f_%d", i)
			tbl := NewTableNameWithDefaultNS(tblName)
			tblDesc := NewTableDesciptor(tbl)
			cf := NewColumnFamilyDescriptor("cf")
			tblDesc.AddColumnDesc(cf)
			tblDesc.AddColumnDesc(cf)
			b, err := s.cli.TableExists(tblName)
			c.Assert(err, IsNil)
			if b {
				// Maybe some table is in disabled state, so we must ignore this error.
				s.cli.DisableTable(tbl)
				err = s.cli.DropTable(tbl)
				c.Assert(err, IsNil)
			}
			err = s.cli.CreateTable(tblDesc, nil)
			c.Assert(err, IsNil)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		tblName := fmt.Sprintf("f_%d", i)
		b, err := s.cli.TableExists(tblName)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)

		tbl := NewTableNameWithDefaultNS(tblName)
		err = s.cli.DisableTable(tbl)
		c.Assert(err, IsNil)

		// Wait for table disabled.
		time.Sleep(5 * time.Second)

		p := NewPut([]byte("key"))
		p.AddValue([]byte("cf"), []byte("f"), []byte("value"))
		ok, err := s.cli.Put(tblName, p)
		c.Assert(err, NotNil)
		c.Assert(ok, IsFalse)

		err = s.cli.EnableTable(tbl)
		c.Assert(err, IsNil)

		ok, err = s.cli.Put(tblName, p)
		c.Assert(err, IsNil)
		c.Assert(ok, IsTrue)

		g := NewGet([]byte("key"))
		g.AddColumn([]byte("cf"), []byte("f"))
		r, err := s.cli.Get(tblName, g)
		c.Assert(err, IsNil)
		c.Assert(r.Columns["cf:f"].Values, HasLen, 1)
		c.Assert(string(r.SortedColumns[0].Value), Equals, "value")

		err = s.cli.DisableTable(tbl)
		c.Assert(err, IsNil)

		err = s.cli.DropTable(tbl)
		c.Assert(err, IsNil)
	}

	// Test disable unexisted table.
	tbl := NewTableNameWithDefaultNS(s.invalidTableName)
	err := s.cli.DisableTable(tbl)
	c.Assert(err, NotNil)

	// Test enable unexisted table.
	err = s.cli.EnableTable(tbl)
	c.Assert(err, NotNil)
}

func (s *AdminTestSuit) TestGetPauseTime(c *C) {
	for i := range retryPauseTime {
		c.Assert(getPauseTime(i), Equals, retryPauseTime[i]*defaultRetryWaitMs)
	}

	invalidRetry := -1
	c.Assert(getPauseTime(invalidRetry), Equals, retryPauseTime[0]*defaultRetryWaitMs)
	invalidRetry = len(retryPauseTime)
	c.Assert(getPauseTime(invalidRetry), Equals, retryPauseTime[len(retryPauseTime)-1]*defaultRetryWaitMs)
}
