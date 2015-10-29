package hbase

import (
	"strconv"

	"github.com/c4pt0r/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
	. "gopkg.in/check.v1"
)

type ResultTestSuit struct{}

var _ = Suite(&ResultTestSuit{})

func (s *ResultTestSuit) TestResultRow(c *C) {
	var cells []*proto.Cell
	for i := 1; i <= 10; i++ {
		cell := &proto.Cell{
			Row:       []byte("row"),
			Family:    []byte("cf"),
			Qualifier: []byte("q"),
			Timestamp: pb.Uint64(uint64(i)),
			CellType:  proto.CellType_PUT.Enum(),
			Value:     []byte(strconv.Itoa(i)),
		}
		cells = append(cells, cell)
	}
	r := &proto.Result{
		Cell: cells,
	}

	rr := NewResultRow(r)
	c.Assert(len(rr.SortedColumns), Equals, 1)
	c.Assert(len(rr.SortedColumns[0].Values), Equals, 10)
	c.Assert(string(rr.SortedColumns[0].Value), Equals, "10")
}
