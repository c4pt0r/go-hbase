package hbase

import (
	"bytes"

	"github.com/c4pt0r/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
)

func incrementByteString(d []byte, i int) []byte {
	r := make([]byte, len(d))
	copy(r, d)
	if i < 0 {
		return append(make([]byte, 1), r...)
	}
	r[i]++
	return r
}

type Scan struct {
	client       *client
	id           uint64
	table        []byte
	StartRow     []byte
	StopRow      []byte
	families     [][]byte
	qualifiers   [][][]byte
	nextStartRow []byte
	numCached    int
	closed       bool
	location     *regionInfo
	server       *connection
	cache        []*ResultRow
	attrs        map[string][]byte
}

func NewScan(table []byte, c HBaseClient) *Scan {
	return &Scan{
		client:       c.(*client),
		table:        table,
		nextStartRow: nil,
		families:     make([][]byte, 0),
		qualifiers:   make([][][]byte, 0),
		numCached:    100,
		closed:       false,
		attrs:        make(map[string][]byte),
	}
}

func (s *Scan) Close() {
	if s.closed == false {
		s.closeScan(s.server, s.location, s.id)
		s.closed = true
	}
}

func (s *Scan) AddStringColumn(family, qual string) {
	s.AddColumn([]byte(family), []byte(qual))
}

func (s *Scan) AddStringFamily(family string) {
	s.AddFamily([]byte(family))
}

func (s *Scan) AddColumn(family, qual []byte) {
	s.AddFamily(family)

	pos := s.posOfFamily(family)

	s.qualifiers[pos] = append(s.qualifiers[pos], qual)
}

func (s *Scan) AddFamily(family []byte) {
	pos := s.posOfFamily(family)

	if pos == -1 {
		s.families = append(s.families, family)
		s.qualifiers = append(s.qualifiers, make([][]byte, 0))
	}
}

func (s *Scan) posOfFamily(family []byte) int {
	for p, v := range s.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (s *Scan) AddAttr(name string, val []byte) {
	s.attrs[name] = val
}

func (s *Scan) Closed() bool {
	return s.closed
}

func (s *Scan) CreateGetFromScan(row []byte) *Get {
	g := NewGet(row)
	for i, family := range s.families {
		if len(s.qualifiers[i]) > 0 {
			for _, qual := range s.qualifiers[i] {
				g.AddColumn(family, qual)
			}
		} else {
			g.AddFamily(family)
		}
	}
	return g
}

func (s *Scan) getData(nextStart []byte) []*ResultRow {
	if s.closed {
		return nil
	}

	server, location := s.getServerAndLocation(s.table, nextStart)

	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(location.name),
		},
		NumberOfRows: pb.Uint32(uint32(s.numCached)),
		Scan:         &proto.Scan{},
	}

	// set attributes
	var attrs []*proto.NameBytesPair
	for k, v := range s.attrs {
		p := &proto.NameBytesPair{
			Name:  pb.String(k),
			Value: v,
		}
		attrs = append(attrs, p)
	}
	if len(attrs) > 0 {
		req.Scan.Attribute = attrs
	}

	if s.id > 0 {
		req.ScannerId = pb.Uint64(s.id)
	}
	if s.StartRow != nil {
		req.Scan.StartRow = s.StartRow
	}
	if s.StopRow != nil {
		req.Scan.StopRow = s.StopRow
	}

	for i, v := range s.families {
		req.Scan.Column = append(req.Scan.Column, &proto.Column{
			Family:    v,
			Qualifier: s.qualifiers[i],
		})
	}
	cl := newCall(req)
	server.call(cl)
	select {
	case msg := <-cl.responseCh:
		return s.processResponse(msg)
	}
}

var lastRegionRows int = 0

func (s *Scan) processResponse(response pb.Message) []*ResultRow {
	var res *proto.ScanResponse
	switch r := response.(type) {
	case *proto.ScanResponse:
		res = r
	default:
		log.Error("Invalid response returned: %T", response)
		return nil
	}

	nextRegion := true
	s.nextStartRow = nil
	s.id = res.GetScannerId()

	results := res.GetResults()
	n := len(results)

	lastRegionRows += n

	if (n == s.numCached) ||
		len(s.location.endKey) == 0 ||
		(s.StopRow != nil && bytes.Compare(s.location.endKey, s.StopRow) > 0 && n < s.numCached) ||
		res.GetMoreResultsInRegion() {
		nextRegion = false
	}

	if n < s.numCached {
		s.nextStartRow = incrementByteString(s.location.endKey, len(s.location.endKey)-1)
	}

	if nextRegion {
		s.closeScan(s.server, s.location, s.id)
		s.server = nil
		s.location = nil
		s.id = 0
		lastRegionRows = 0
	}

	if n == 0 && !nextRegion {
		s.Close()
	}

	tbr := make([]*ResultRow, n)
	for i, v := range results {
		tbr[i] = NewResultRow(v)
	}

	return tbr
}

func (s *Scan) nextBatch() int {
	startRow := s.nextStartRow
	if startRow == nil {
		startRow = s.StartRow
	}
	rs := s.getData(startRow)
	if rs == nil || len(rs) == 0 {
		return 0
	}
	s.cache = rs
	return len(s.cache)
}

func (s *Scan) Next() *ResultRow {
	var ret *ResultRow
	if len(s.cache) == 0 {
		n := s.nextBatch()
		// no data returned
		if n == 0 {
			return nil
		}
	}
	ret = s.cache[0]
	s.cache = s.cache[1:len(s.cache)]
	return ret
}

func (s *Scan) closeScan(server *connection, location *regionInfo, id uint64) {
	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(location.name),
		},
		ScannerId:    pb.Uint64(id),
		CloseScanner: pb.Bool(true),
	}
	cl := newCall(req)
	server.call(cl)
	<-cl.responseCh
}

func (s *Scan) getServerAndLocation(table, startRow []byte) (server *connection, location *regionInfo) {
	if s.server != nil && s.location != nil {
		server = s.server
		location = s.location
		return
	}
	location = s.client.locateRegion(table, startRow, true)
	server = s.client.getRegionConn(location.server)

	s.server = server
	s.location = location
	return
}
