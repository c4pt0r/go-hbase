package hbase

type TableDescriptor struct {
	name      string
	nameSpace string
	cfs       []*ColumnFamilyDescriptor
}

func NewTableDesciptor(namespace, name string) *TableDescriptor {
	return &TableDescriptor{
		name:      name,
		nameSpace: namespace,
	}
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

func NewColumnFamilyDescriptor(name string) *ColumnFamilyDescriptor {
	return &ColumnFamilyDescriptor{
		name:  name,
		attrs: make(map[string][]byte),
	}
}

/*
func (c *client) CreateTable(t *TableDescriptor, splits [][]byte) error {
	req := &proto.CreateTableRequest{}
	schema := &proto.TableSchema{}

	schema.TableName = &proto.TableName{
		Qualifier: []byte(t.name),
		Namespace: []byte("default"),
	}
	if len(t.nameSpace) != 0 {
		schema.TableName.Namespace = []byte(t.nameSpace)
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

	log.Info(resp)
	return nil
}
*/

func (c *client) TableExists(tbl string) bool {
	scan := NewScan(metaTableName, c)
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
		if region != nil && region.tableName == tbl {
			return true
		}
	}
	return false
}
