package hbase

import (
	"runtime"
	"strconv"
	"sync"

	. "gopkg.in/check.v1"
)

type ScanTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&ScanTestSuit{})

func (s *ScanTestSuit) SetUpSuite(c *C) {
	var err error
	s.cli, err = NewClient([]string{"localhost"}, "/hbase")
	c.Assert(err, Equals, nil)
	for i := 1; i <= 5; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))
		s.cli.Put("t2", p)
	}
}

func (s *ScanTestSuit) TearDownSuite(c *C) {
	for i := 1; i <= 5; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		d.AddColumn([]byte("cf"), []byte("q"))
		s.cli.Delete("t2", d)
	}
}

func (s *ScanTestSuit) TestScan(c *C) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scan := NewScan([]byte("t2"), s.cli)
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
			c.Assert(cnt, Equals, 4)
		}()
	}

	wg.Wait()
}
