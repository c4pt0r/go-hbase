package hbase

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"

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
	c.Assert(err, IsNil)

	tblDesc := NewTableDesciptor(NewTableNameWithDefaultNS("t1"))
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	err = s.cli.CreateTable(tblDesc, nil)
	c.Assert(err, IsNil)
}

func (s *HBaseGetTestSuit) TearDownTest(c *C) {
	err := s.cli.DisableTable(NewTableNameWithDefaultNS("t1"))
	c.Assert(err, IsNil)

	err = s.cli.DropTable(NewTableNameWithDefaultNS("t1"))
	c.Assert(err, IsNil)
}

func (s *HBaseGetTestSuit) TestGet(c *C) {
	g := NewGet([]byte("row"))
	g.AddFamily([]byte("cf"))
	g.AddColumn([]byte("cf"), []byte("c"))
	g.AddColumn([]byte("cf"), []byte("v"))
	g.AddFamily([]byte("cf1"))

	msg := g.ToProto()
	p, _ := msg.(*proto.Get)

	c.Assert(p.Column, HasLen, 2)

	for _, col := range p.Column {
		if bytes.Compare([]byte("cf"), col.Family) == 0 {
			c.Assert(col.Qualifier, HasLen, 2)
		} else {
			c.Assert(col.Qualifier, HasLen, 0)
		}
	}
}

func (s *HBaseGetTestSuit) TestGetWithClient(c *C) {
	// get item not exists
	g := NewGet([]byte("nosuchrow"))
	r, err := s.cli.Get("nosuchtable", g)
	c.Assert(err, NotNil)
	c.Assert(r, IsNil)

	r, err = s.cli.Get("t1", g)
	c.Assert(r, IsNil)
	c.Assert(err, IsNil)
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
			c.Assert(b, IsTrue)
			c.Assert(err, IsNil)
		}(i)
	}
	wg.Wait()

	g := NewGet([]byte("test"))
	_, err := s.cli.Get("t2", g)
	c.Assert(err, IsNil)
}
