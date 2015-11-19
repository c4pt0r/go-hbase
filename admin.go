package hbase

import (
	"sort"
	"strconv"
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
	i := retry
	if i > len(retryPauseTime) {
		i = len(retryPauseTime) - 1
	}
	return retryPauseTime[i] * defaultRetryWaitMs
}

func (c *client) CreateTable(t *TableDescriptor, splits [][]byte) error {
	req := &proto.CreateTableRequest{}
	schema := &proto.TableSchema{}

	sort.Sort(BytesSlice(splits))

	schema.TableName = &proto.TableName{
		Qualifier: []byte(t.name.name),
		Namespace: []byte(t.name.namespace),
	}
	if len(t.attrs) != 0 {
		for k, v := range t.attrs {
			schema.Attributes = append(schema.Attributes, &proto.BytesBytesPair{
				First:  []byte(k),
				Second: []byte(v),
			})
		}
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

	ch := c.adminAction(req)
	resp := <-ch
	switch r := resp.(type) {
	case *exception:
		return errors.New(r.msg)
	}
	// wait and check
	for retry := 0; retry < defaultMaxActionRetries*retryLongerMultiplier; retry++ {
		numRegs := 1
		if splits != nil {
			numRegs = len(splits) + 1
		}
		regCnt := 0
		c.metaScan(t.name.name, func(r *RegionInfo) (bool, error) {
			if !(r.Offline || r.Split) && len(r.Server) > 0 && r.TableName == t.name.name {
				regCnt++
			}
			return true, nil
		})
		if regCnt == numRegs {
			return nil
		}
		log.Info("sleep and try again")
		time.Sleep(time.Duration(getPauseTime(retry)) * time.Millisecond)
	}
	return errors.Errorf("create table timeout")
}

func (c *client) DisableTable(tblName TableName) error {
	req := &proto.DisableTableRequest{
		TableName: &proto.TableName{
			Qualifier: []byte(tblName.name),
			Namespace: []byte(tblName.namespace),
		},
	}
	ch := c.adminAction(req)
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
	ch := c.adminAction(req)
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
	ch := c.adminAction(req)
	resp := <-ch
	switch r := resp.(type) {
	case *exception:
		return errors.New(r.msg)
	}
	return nil
}

func (c *client) metaScan(tbl string, fn func(r *RegionInfo) (bool, error)) error {
	scan := NewScan(metaTableName, 0, c)
	if scan != nil {
		defer scan.Close()
	}

	startRow := []byte(tbl)
	stopRow := incrementByteString([]byte(tbl), len([]byte(tbl))-1)

	scan.StartRow = startRow
	scan.StopRow = stopRow

	for {
		r := scan.Next()
		if r == nil || scan.Closed() {
			break
		}
		region := c.parseRegion(r)
		if more, err := fn(region); !more || err != nil {
			return err
		}
	}
	return nil
}

func (c *client) TableExists(tbl string) bool {
	found := false
	c.metaScan(tbl, func(region *RegionInfo) (bool, error) {
		if region != nil && region.TableName == tbl {
			found = true
			return false, nil
		}
		return true, nil
	})
	return found
}
