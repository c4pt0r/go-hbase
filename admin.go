package hbase

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase/proto"
)

type TableName struct {
	namespace string
	name      string
}

func NewTableNameWithDefaultNS(tblName string) TableName {
	return TableName{
		namespace: "default",
		name:      tblName,
	}
}

type TableDescriptor struct {
	name  TableName
	attrs map[string][]byte
	cfs   []*ColumnFamilyDescriptor
}

func NewTableDesciptor(tblName TableName) *TableDescriptor {
	ret := &TableDescriptor{
		name:  tblName,
		attrs: map[string][]byte{},
	}
	ret.AddStrAddr("IS_META", "false")
	return ret
}

func (c *TableDescriptor) AddStrAddr(attrName string, val string) {
	c.attrs[attrName] = []byte(val)
}

func (t *TableDescriptor) AddColumnDesc(cf *ColumnFamilyDescriptor) {
	for _, c := range t.cfs {
		if c.name == cf.name {
			return
		}
	}
	t.cfs = append(t.cfs, cf)
}

type ColumnFamilyDescriptor struct {
	name  string
	attrs map[string][]byte
}

func (c *ColumnFamilyDescriptor) AddAttr(attrName string, val []byte) {
	c.attrs[attrName] = val
}

func (c *ColumnFamilyDescriptor) AddStrAddr(attrName string, val string) {
	c.attrs[attrName] = []byte(val)
}

// Themis will use VERSIONS=1 for some hook.
func NewColumnFamilyDescriptor(name string) *ColumnFamilyDescriptor {
	return newColumnFamilyDescriptor(name, 1)
}

func newColumnFamilyDescriptor(name string, versionsNum int) *ColumnFamilyDescriptor {
	versions := strconv.Itoa(versionsNum)

	ret := &ColumnFamilyDescriptor{
		name:  name,
		attrs: make(map[string][]byte),
	}

	// add default attrs
	ret.AddStrAddr("DATA_BLOCK_ENCODING", "NONE")
	ret.AddStrAddr("BLOOMFILTER", "ROW")
	ret.AddStrAddr("REPLICATION_SCOPE", "0")
	ret.AddStrAddr("COMPRESSION", "NONE")
	ret.AddStrAddr("VERSIONS", versions)
	ret.AddStrAddr("TTL", "2147483647") // 1 << 31
	ret.AddStrAddr("MIN_VERSIONS", "0")
	ret.AddStrAddr("KEEP_DELETED_CELLS", "false")
	ret.AddStrAddr("BLOCKSIZE", "65536")
	ret.AddStrAddr("IN_MEMORY", "false")
	ret.AddStrAddr("BLOCKCACHE", "true")
	return ret
}

func getPauseTime(retry int) int64 {
	if retry >= len(retryPauseTime) {
		retry = len(retryPauseTime) - 1
	}
	return retryPauseTime[retry] * defaultRetryWaitMs
}

func (c *client) CreateTable(t *TableDescriptor, splits [][]byte) error {
	req := &proto.CreateTableRequest{}
	schema := &proto.TableSchema{}

	sort.Sort(BytesSlice(splits))

	schema.TableName = &proto.TableName{
		Qualifier: []byte(t.name.name),
		Namespace: []byte(t.name.namespace),
	}

	for k, v := range t.attrs {
		schema.Attributes = append(schema.Attributes, &proto.BytesBytesPair{
			First:  []byte(k),
			Second: []byte(v),
		})
	}

	for _, c := range t.cfs {
		cf := &proto.ColumnFamilySchema{
			Name: []byte(c.name),
		}
		for k, v := range c.attrs {
			cf.Attributes = append(cf.Attributes, &proto.BytesBytesPair{
				First:  []byte(k),
				Second: []byte(v),
			})
		}
		schema.ColumnFamilies = append(schema.ColumnFamilies, cf)
	}

	req.TableSchema = schema
	req.SplitKeys = splits

	ch, err := c.adminAction(req)
	if err != nil {
		return errors.Trace(err)
	}

	resp := <-ch
	switch r := resp.(type) {
	case *exception:
		return errors.New(r.msg)
	}

	// wait and check
	for retry := 0; retry < defaultMaxActionRetries*retryLongerMultiplier; retry++ {
		regCnt := 0
		numRegs := len(splits) + 1
		err = c.metaScan(t.name.name, func(r *RegionInfo) (bool, error) {
			if !(r.Offline || r.Split) && len(r.Server) > 0 && r.TableName == t.name.name {
				regCnt++
			}
			return true, nil
		})
		if err != nil {
			return errors.Trace(err)
		}

		if regCnt == numRegs {
			return nil
		}
		log.Warnf("Retrying create table for the %d time(s)", retry+1)
		time.Sleep(time.Duration(getPauseTime(retry)) * time.Millisecond)
	}
	return errors.New("create table timeout")
}

func (c *client) DisableTable(tblName TableName) error {
	req := &proto.DisableTableRequest{
		TableName: &proto.TableName{
			Qualifier: []byte(tblName.name),
			Namespace: []byte(tblName.namespace),
		},
	}

	ch, err := c.adminAction(req)
	if err != nil {
		return errors.Trace(err)
	}

	resp := <-ch
	switch r := resp.(type) {
	case *exception:
		return errors.New(r.msg)
	}

	return nil
}

func (c *client) EnableTable(tblName TableName) error {
	req := &proto.EnableTableRequest{
		TableName: &proto.TableName{
			Qualifier: []byte(tblName.name),
			Namespace: []byte(tblName.namespace),
		},
	}

	ch, err := c.adminAction(req)
	if err != nil {
		return errors.Trace(err)
	}

	resp := <-ch
	switch r := resp.(type) {
	case *exception:
		return errors.New(r.msg)
	}

	return nil
}

func (c *client) DropTable(tblName TableName) error {
	req := &proto.DeleteTableRequest{
		TableName: &proto.TableName{
			Qualifier: []byte(tblName.name),
			Namespace: []byte(tblName.namespace),
		},
	}

	ch, err := c.adminAction(req)
	if err != nil {
		return errors.Trace(err)
	}

	resp := <-ch
	switch r := resp.(type) {
	case *exception:
		return errors.New(r.msg)
	}

	return nil
}

func (c *client) metaScan(tbl string, fn func(r *RegionInfo) (bool, error)) error {
	scan := NewScan(metaTableName, 0, c)
	defer scan.Close()

	scan.StartRow = []byte(tbl)
	scan.StopRow = nextKey([]byte(tbl))

	for {
		r := scan.Next()
		if r == nil || scan.Closed() {
			break
		}

		region, err := c.parseRegion(r)
		if err != nil {
			return errors.Trace(err)
		}

		if more, err := fn(region); !more || err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *client) TableExists(tbl string) (bool, error) {
	found := false
	err := c.metaScan(tbl, func(region *RegionInfo) (bool, error) {
		if region.TableName == tbl {
			found = true
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}

	return found, nil
}

// Split split region
// tblOrRegion table name or region(<tbl>,<endKey>,<timestamp>.<md5>)
// splitPoint split point which is a key, leave "" if want to split automatically
func (c *client) Split(tblOrRegion, splitPoint string) error {
	// extract table name from supposing regionName
	tbls := strings.SplitN(tblOrRegion, ",", 2)
	tbl := tbls[0]
	found := false
	var foundRegion *RegionInfo
	c.metaScan(tbl, func(region *RegionInfo) (bool, error) {
		if region != nil && region.Name == tblOrRegion {
			found = true
			foundRegion = region
			return false, nil
		}
		return true, nil
	})
	// this is a region name, split it directly
	if found {
		return c.split(foundRegion, []byte(splitPoint))
	}
	// this may be a table name
	tbl = tblOrRegion
	regions, err := c.GetRegions([]byte(tbl), false)
	if err != nil {
		return errors.Trace(err)
	}
	// split all region
	for _, region := range regions {
		err := c.split(region, []byte(splitPoint))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) split(region *RegionInfo, splitPoint []byte) error {
	// not in this region, skip it
	if len(splitPoint) > 0 && !FindKey(region, splitPoint) {
		return nil
	}
	c.CleanRegionCache([]byte(region.TableName))
	rs := NewRegionSpecifier(region.Name)
	req := &proto.SplitRegionRequest{
		Region: rs,
	}
	if len(splitPoint) > 0 {
		req.SplitPoint = splitPoint
	}
	// empty response
	_, err := c.regionAction(region.Server, req)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
