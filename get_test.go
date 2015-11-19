package hbase

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/go-hbase/proto"
)

type HBaseGetTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&HBaseGetTestSuit{})

func (s *HBaseGetTestSuit) SetUpTest(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, Equals, nil)

	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("t1"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	s.cli.CreateTable(tblDesc, nil)
	log.Info("create table")
}

func (s *HBaseGetTestSuit) TearDownTest(c *C) {
	s.cli.DisableTable(NewTableNameWithDefaultNS("t1"))
	s.cli.DropTable(NewTableNameWithDefaultNS("t1"))
}

func (s *HBaseGetTestSuit) TestGet(c *C) {
	g := NewGet([]byte("row"))
	g.AddFamily([]byte("cf"))
	g.AddColumn([]byte("cf"), []byte("c"))
	g.AddColumn([]byte("cf"), []byte("v"))
	g.AddFamily([]byte("cf1"))

	msg := g.ToProto()
	p, _ := msg.(*proto.Get)

	c.Assert(len(p.Column), Equals, 2)

	for _, col := range p.Column {
		if bytes.Compare([]byte("cf"), col.Family) == 0 {
			c.Assert(len(col.Qualifier), Equals, 2)
		} else {
			c.Assert(len(col.Qualifier), Equals, 0)
		}
	}
}

func (s *HBaseGetTestSuit) TestWithClient(c *C) {
	// get item not exists
	g := NewGet([]byte("nosuchrow"))
	r, err := s.cli.Get("nosuchtable", g)
	c.Assert(err.Error(), Equals, "Create region server connection failed")
	c.Assert(r == nil, Equals, true)

	r, err = s.cli.Get("t1", g)
	c.Assert(r == nil, Equals, true)
	c.Assert(err, Equals, nil)
}

func (s *HBaseGetTestSuit) TestConcurrentGet(c *C) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p := NewPut([]byte("test"))
			p.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))
			b, err := s.cli.Put("t2", p)
			c.Assert(b, Equals, true)
			c.Assert(err, Equals, nil)
		}(i)
	}
	wg.Wait()

	g := NewGet([]byte("test"))
	_, err := s.cli.Get("t2", g)
	c.Assert(err, Equals, nil)
}
