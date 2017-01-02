package hbase

import . "github.com/pingcap/check"

type ClientTestSuit struct {
	cli HBaseClient
}

var _ = Suite(&ClientTestSuit{})

func (s *ClientTestSuit) SetUpSuite(c *C) {
}

func (s *ClientTestSuit) TearDownSuite(c *C) {
}

func (s *ClientTestSuit) TestCreateRegionName(c *C) {
	table := "t"
	startKey := "key"
	id := "1234"
	newFormat := false
	mockClient := &client{}

	name := mockClient.createRegionName([]byte(table), []byte(startKey), id, newFormat)
	c.Assert(string(name), Equals, "t,key,1234")

	startKey = ""
	name = mockClient.createRegionName([]byte(table), []byte(startKey), id, newFormat)
	c.Assert(string(name), Equals, "t,\x00,1234")

	newFormat = true
	name = mockClient.createRegionName([]byte(table), []byte(startKey), id, newFormat)
	c.Assert(string(name), Equals, "t,\x00,1234.0b33d053d09ba72744f058890ed449b2.")
}
