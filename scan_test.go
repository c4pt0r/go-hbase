package hbase

import (
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/log"
	. "gopkg.in/check.v1"
)

type ScanTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&ScanTestSuit{})

func (s *ScanTestSuit) SetUpSuite(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, Equals, nil)
	log.Info("create table")
	s.cli.DropTable(NewTableNameWithDefaultNS("scan_test"))
	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("scan_test"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	s.cli.CreateTable(tblDesc, nil)

	for i := 1; i <= 10000; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))
		s.cli.Put("scan_test", p)
	}
}

func (s *ScanTestSuit) TestScanInSplit(c *C) {
	for {
		break
		log.Info("begin scan")
		scan := NewScan([]byte("scan_test"), 100, s.cli)
		for {
			r := scan.Next()
			if r == nil || scan.Closed() {
				break
			}
		}
		if scan.Error() != nil {
			log.Fatal(scan.Error())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *ScanTestSuit) TearDownSuite(c *C) {
	for i := 1; i <= 5; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		d.AddColumn([]byte("cf"), []byte("q"))
		s.cli.Delete("scan_test", d)
	}
}

func (s *ScanTestSuit) TestScan(c *C) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scan := NewScan([]byte("scan_test"), 100, s.cli)
			defer scan.Close()
			// [1, 5)
			scan.StartRow = []byte("1")
			scan.StopRow = []byte("5")

			cnt := 0
			for {
				r := scan.Next()
				if r == nil || scan.Closed() {
					break
				}
				cnt++
			}
			c.Assert(cnt, Equals, 4445)
		}()
	}

	wg.Wait()
}
