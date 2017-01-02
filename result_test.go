package hbase

import (
	"strconv"

	pb "github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/go-hbase/proto"
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
	c.Assert(rr.SortedColumns, HasLen, 1)
	c.Assert(rr.SortedColumns[0].Values, HasLen, 10)
	c.Assert(string(rr.SortedColumns[0].Value), Equals, "10")
}
