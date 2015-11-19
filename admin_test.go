package hbase

import (
	"flag"
	"fmt"
	"strings"
	"sync"

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

	for i := 0; i < 10; i++ {
		tbl := NewTableNameWithDefaultNS(fmt.Sprintf("f_%d", i))
		cli.DisableTable(tbl)
		cli.DropTable(tbl)
	}
}
