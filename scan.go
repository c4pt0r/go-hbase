package hbase

import (
	"bytes"
	"errors"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase/proto"
)

func incrementByteString(d []byte, i int) []byte {
	r := make([]byte, len(d))
	copy(r, d)
	// in case of: len(nil) - 1, e.g: scan(startKey, nil)
	if i < 0 {
		return append(make([]byte, 1), r...)
	}
	r[i]++
	// carry handle
	for i > 0 {
		if r[i] == 0 {
			i--
			r[i]++
		} else {
			break
		}
	}
	// i == 0, need add a new byte before array
	if r[i] == 0 {
		r = append(make([]byte, 1), r...)
		r[0]++
	}
	return r
}

const (
	defaultScanMaxRetries = 3
)

type Scan struct {
	client *client
	id     uint64
	table  []byte
	// row key
	StartRow     []byte
	StopRow      []byte
	families     [][]byte
	qualifiers   [][][]byte
	nextStartKey []byte
	numCached    int
	closed       bool
	location     *RegionInfo
	server       *connection
	cache        []*ResultRow
	attrs        map[string][]byte
	MaxVersions  uint32
	TsRangeFrom  uint64
	TsRangeTo    uint64
	lastResult   *ResultRow
	// if region split, set startKey = lastResult.Row, but must skip the first
	skipFirst  bool
	err        error
	maxRetries int
}

func NewScan(table []byte, batchSize int, c HBaseClient) *Scan {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Scan{
		client:       c.(*client),
		table:        table,
		nextStartKey: nil,
		families:     make([][]byte, 0),
		qualifiers:   make([][][]byte, 0),
		numCached:    batchSize,
		closed:       false,
		attrs:        make(map[string][]byte),
		maxRetries:   defaultScanMaxRetries,
	}
}

func (s *Scan) Close() {
	if s.closed {
		return
	}

	if s.server != nil && s.location != nil {
		s.closeScan(s.server, s.location, s.id)
	}
	s.closed = true
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

func (s *Scan) AddTimeRange(from uint64, to uint64) {
	s.TsRangeFrom = from
	s.TsRangeTo = to
}

func (s *Scan) Closed() bool {
	return s.closed
}

func (s *Scan) Error() error {
	return s.err
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

func (s *Scan) getData(startKey []byte, retries int) []*ResultRow {
	if s.closed {
		return nil
	}

	server, location := s.getServerAndLocation(s.table, startKey)

	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(location.Name),
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
	req.Scan.StartRow = startKey
	if s.StopRow != nil {
		req.Scan.StopRow = s.StopRow
	}
	if s.MaxVersions > 0 {
		req.Scan.MaxVersions = &s.MaxVersions
	}
	if s.TsRangeFrom >= 0 && s.TsRangeTo > 0 && s.TsRangeTo > s.TsRangeFrom {
		req.Scan.TimeRange = &proto.TimeRange{
			From: pb.Uint64(s.TsRangeFrom),
			To:   pb.Uint64(s.TsRangeTo),
		}
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
		rs := s.processResponse(msg)
		if s.err != nil && (isNotInRegionError(s.err) || isUnknownScannerError(s.err)) {
			if retries <= s.maxRetries {
				// clean this table region cache and try again
				s.client.CleanRegionCache(s.table)
				// create new scanner and set startRow to lastResult
				s.id = 0
				if s.lastResult != nil {
					startKey = s.lastResult.Row
					s.skipFirst = true
				}
				s.server = nil
				s.location = nil
				s.err = nil
				log.Warnf("Retryint get data for %d time(s)", retries+1)
				RetrySleep(retries + 1)
				return s.getData(startKey, retries+1)
			}
		}
		return rs
	}
}

func (s *Scan) processResponse(response pb.Message) []*ResultRow {
	var res *proto.ScanResponse
	switch r := response.(type) {
	case *proto.ScanResponse:
		res = r
	default:
		s.err = errors.New(response.(*exception).msg)
		return nil
	}

	nextRegion := true
	s.nextStartKey = nil
	s.id = res.GetScannerId()

	results := res.GetResults()
	n := len(results)

	if (n == s.numCached) ||
		len(s.location.EndKey) == 0 ||
		(s.StopRow != nil && bytes.Compare(s.location.EndKey, s.StopRow) > 0 && n < s.numCached) ||
		res.GetMoreResultsInRegion() {
		nextRegion = false
	}

	if nextRegion {
		s.nextStartKey = s.location.EndKey
		s.closeScan(s.server, s.location, s.id)
		s.server = nil
		s.location = nil
		s.id = 0
	}

	if n == 0 && !nextRegion {
		s.Close()
	}

	if s.skipFirst {
		results = results[1:]
		s.skipFirst = false
		n = len(results)
	}
	tbr := make([]*ResultRow, n)
	for i, v := range results {
		tbr[i] = NewResultRow(v)
	}

	return tbr
}

func (s *Scan) nextBatch() int {
	startKey := s.nextStartKey
	if startKey == nil {
		startKey = s.StartRow
	}
	rs := s.getData(startKey, 0)
	// current region get 0 data, switch next region
	if rs != nil && len(rs) == 0 && s.nextStartKey != nil {
		rs = s.getData(s.nextStartKey, 0)
	}
	if rs == nil {
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
	s.lastResult = ret
	s.cache = s.cache[1:]
	return ret
}

func (s *Scan) closeScan(server *connection, location *RegionInfo, id uint64) {
	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(location.Name),
		},
		ScannerId:    pb.Uint64(id),
		CloseScanner: pb.Bool(true),
	}
	cl := newCall(req)
	server.call(cl)
	<-cl.responseCh
}

func (s *Scan) getServerAndLocation(table, startRow []byte) (server *connection, location *RegionInfo) {
	if s.server != nil && s.location != nil {
		server = s.server
		location = s.location
		return
	}
	location = s.client.LocateRegion(table, startRow, true)
	server = s.client.getRegionConn(location.Server)

	s.server = server
	s.location = location
	return
}
